use std::{collections::HashMap, path::PathBuf, sync::Arc};

use crate::{
    deletionmap::{DeletionMap, ImmutableDeletionMap, MutableDeletionMap},
    index::IndexSerializerFactory,
    schema::SchemaRef,
    table::{
        segment::{SegmentColumnSerializer, SegmentMetaData},
        SegmentStat,
    },
    Directory, DocId, END_DOCID,
};

use super::{
    segment::{
        BuildingSegment, BuildingSegmentData, PersistentSegment, PersistentSegmentData,
        SegmentMerger,
    },
    SegmentId, SegmentMeta, TableSettingsRef, Version,
};

#[derive(Clone)]
pub struct TableData {
    building_segments: Vec<BuildingSegment>,
    persistent_segments: Vec<PersistentSegment>,
    recent_segment_stat: Option<Arc<SegmentStat>>,
    deletionmap: DeletionMap,
    version: Version,
    directory: Box<dyn Directory>,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableDataRef = Arc<TableData>;

impl TableData {
    pub fn open<D: Into<Box<dyn Directory>>>(
        directory: D,
        schema: SchemaRef,
        settings: TableSettingsRef,
    ) -> Self {
        let directory: Box<dyn Directory> = directory.into();
        let version = Version::load_lastest(directory.as_ref()).unwrap();
        let mut persistent_segments = vec![];
        let mut base_docid = 0;
        let mut total_doc_count = 0;
        for segment_id in version.segments() {
            let segment = Arc::new(PersistentSegmentData::open(
                directory.as_ref(),
                segment_id.clone(),
                &schema,
            ));
            let doc_count = segment.doc_count();
            let meta = SegmentMeta::new(segment.segment_id().clone(), base_docid, doc_count);
            persistent_segments.push(PersistentSegment::new(meta, segment));
            base_docid += doc_count as DocId;
            total_doc_count += doc_count;
        }

        let mut deletionmap = MutableDeletionMap::with_doc_count(total_doc_count);
        for seg in &persistent_segments {
            deletionmap.copy_immutable_at(
                seg.data().deletionmap(),
                seg.meta().base_docid(),
                seg.meta().doc_count(),
            );
        }
        let immutable_deletionmap: ImmutableDeletionMap = deletionmap.into();

        Self {
            building_segments: vec![],
            persistent_segments,
            recent_segment_stat: None,
            deletionmap: immutable_deletionmap.into(),
            version,
            directory,
            schema,
            settings,
        }
    }

    pub fn reload(&mut self) {
        let version = Version::load_lastest(self.directory.as_ref()).unwrap();
        if self.version != version {
            let current_segments_map: HashMap<_, _> = self
                .persistent_segments
                .iter()
                .map(|segment| (segment.data().segment_id().clone(), segment.data().clone()))
                .collect();
            let mut segments = vec![];
            let mut base_docid = 0;
            for segment_id in version.segments() {
                let segment = if let Some(segment) = current_segments_map.get(segment_id) {
                    segment.clone()
                } else {
                    Arc::new(PersistentSegmentData::open(
                        self.directory.as_ref(),
                        segment_id.clone(),
                        &self.schema,
                    ))
                };

                let meta = SegmentMeta::new(segment_id.clone(), base_docid, segment.doc_count());
                segments.push(PersistentSegment::new(meta, segment.clone()));
                base_docid += segment.doc_count() as DocId;
            }
            std::mem::swap(&mut self.persistent_segments, &mut segments);
            for segment in &mut self.building_segments {
                segment.meta_mut().set_base_docid(base_docid);
                base_docid += segment.meta().doc_count() as DocId;
            }
            self.version = version;
        }
    }

    pub fn recent_segment_stat(&self) -> Option<&Arc<SegmentStat>> {
        self.recent_segment_stat.as_ref()
    }

    pub fn add_building_segment(&mut self, building_segment: Arc<BuildingSegmentData>) {
        let base_docid = self
            .building_segments
            .last()
            .map(|segment| segment.meta().end_docid())
            .or(self
                .persistent_segments
                .last()
                .map(|segment| segment.meta().end_docid()))
            .unwrap_or_default();
        let doc_count = (END_DOCID - base_docid) as usize;
        let meta = SegmentMeta::new(building_segment.segment_id().clone(), base_docid, doc_count);
        let building_segment = BuildingSegment::new(meta, building_segment);
        self.building_segments.push(building_segment);
    }

    pub fn dump_building_segment(&mut self, building_segment_data: Arc<BuildingSegmentData>) {
        assert!(Arc::ptr_eq(
            self.building_segments.last().unwrap().data(),
            &building_segment_data
        ));
        building_segment_data.set_dumping_start();
        let building_segment = self.building_segments.last_mut().unwrap();
        let segment_stat = building_segment.collect_segment_stat();
        self.recent_segment_stat = Some(Arc::new(segment_stat));
        building_segment
            .meta_mut()
            .set_doc_count(building_segment_data.doc_count());
        let total_doc_count = building_segment_data.doc_count();
        let deletionmap = building_segment.data().deletionmap();
        let deleted_doc_count = deletionmap.deleted_doc_count();
        if total_doc_count == deleted_doc_count {
            self.remove_building_segment(building_segment_data);
            return;
        }
        let docid_mapping = if deleted_doc_count > 0 {
            let mut docid_mapping = vec![];
            let mut docid = 0;
            for i in 0..total_doc_count {
                docid_mapping.push(if deletionmap.is_deleted(i as DocId) {
                    None
                } else {
                    let current_docid = docid;
                    docid += 1;
                    Some(current_docid)
                });
            }
            Some(docid_mapping)
        } else {
            None
        };

        let doc_count = total_doc_count - deleted_doc_count;

        let segment_path =
            PathBuf::from("segments").join(building_segment_data.segment_id().as_str());

        let column_data = building_segment_data.column_data().columns();
        let column_serializer = SegmentColumnSerializer {};
        column_serializer.serialize(
            self.directory.as_ref(),
            &segment_path,
            docid_mapping.as_ref(),
            &self.schema,
            column_data,
        );

        let index_path = segment_path.join("index");
        let index_serializer_factory = IndexSerializerFactory::default();
        for index in self.schema.indexes() {
            let index_data = building_segment_data
                .index_data()
                .index_data(index.name())
                .unwrap()
                .clone();
            let index_serializer = index_serializer_factory.create(index, index_data);
            index_serializer.serialize(
                self.directory.as_ref(),
                &index_path,
                docid_mapping.as_ref(),
            );
        }

        let meta = SegmentMetaData::new(doc_count);
        let meta_path = segment_path.join("meta.json");
        meta.save(self.directory.as_ref(), &meta_path);
        let mut version = self.version.next_version();
        version.add_segment(building_segment_data.segment_id().clone());
        version.save(self.directory.as_ref());

        self.remove_building_segment(building_segment_data);
        self.reload();

        if version.segments().len() > 1 {
            let mut version_merged = version.next_version();
            self.merge_segments(&mut version_merged);
        }
    }

    pub fn remove_building_segment(&mut self, building_segment: Arc<BuildingSegmentData>) {
        self.building_segments
            .retain(|segment| !Arc::ptr_eq(segment.data(), &building_segment));
    }

    pub fn merge_segments(&mut self, version: &mut Version) {
        let segment_merger = SegmentMerger::default();
        segment_merger.merge(self.directory.as_ref(), &self.schema, version);

        self.reload();
    }

    pub fn set_deletionmap(&mut self, deletionmap: DeletionMap) {
        self.deletionmap = deletionmap;
    }

    pub fn deletionmap(&self) -> &DeletionMap {
        &self.deletionmap
    }

    pub fn directory(&self) -> &dyn Directory {
        self.directory.as_ref()
    }

    pub fn persistent_segments(&self) -> &[PersistentSegment] {
        &self.persistent_segments
    }

    pub fn active_building_segment(&self) -> Option<&BuildingSegment> {
        self.building_segments
            .last()
            .filter(|seg| !seg.is_dumping())
    }

    pub fn building_segments(&self) -> &[BuildingSegment] {
        &self.building_segments
    }

    pub fn segment_of_docid(&self, docid: DocId) -> Option<(&SegmentId, DocId)> {
        self.persistent_segments
            .iter()
            .find(|segment| docid < segment.meta().end_docid())
            .map(|segment| {
                (
                    segment.meta().segment_id(),
                    segment.meta().inner_docid(docid),
                )
            })
            .or(self
                .building_segments
                .iter()
                .find(|segment| docid < segment.meta().end_docid())
                .map(|segment| {
                    (
                        segment.meta().segment_id(),
                        segment.meta().inner_docid(docid),
                    )
                }))
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn settings(&self) -> &TableSettingsRef {
        &self.settings
    }
}
