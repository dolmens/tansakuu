use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};

use crate::{
    column::ColumnSerializerFactory, index::IndexSerializerFactory, schema::SchemaRef,
    table::segment::SegmentMetaData, DocId,
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
    version: Version,
    directory: PathBuf,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableDataRef = Arc<TableData>;

impl TableData {
    pub fn open(directory: PathBuf, schema: SchemaRef, settings: TableSettingsRef) -> Self {
        let version = Version::load_lastest(&directory);
        let segment_directory = directory.join("segments");
        let mut persistent_segments = vec![];
        let mut base_docid = 0;
        for segment_id in version.segments() {
            let segment = Arc::new(PersistentSegmentData::open(
                segment_id.clone(),
                &schema,
                &segment_directory,
            ));
            let doc_count = segment.doc_count();
            let meta = SegmentMeta::new(segment.segment_id().clone(), base_docid, doc_count);
            persistent_segments.push(PersistentSegment::new(meta, segment));
            base_docid += doc_count as DocId;
        }

        Self {
            building_segments: vec![],
            persistent_segments,
            version,
            directory,
            schema,
            settings,
        }
    }

    pub fn reload(&mut self) {
        let version = Version::load_lastest(&self.directory);
        if self.version != version {
            let segment_directory = self.directory.join("segments");
            let current_segments_map: HashMap<_, _> = self
                .persistent_segments
                .iter()
                .map(|segment| {
                    (
                        segment.segment().segment_id().clone(),
                        segment.segment().clone(),
                    )
                })
                .collect();
            let mut segments = vec![];
            let mut base_docid = 0;
            for segment_id in version.segments() {
                let segment = if let Some(segment) = current_segments_map.get(segment_id) {
                    segment.clone()
                } else {
                    Arc::new(PersistentSegmentData::open(
                        segment_id.clone(),
                        &self.schema,
                        &segment_directory,
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

    pub fn add_building_segment(&mut self, building_segment: Arc<BuildingSegmentData>) {
        if let Some(building_segment) = self.building_segments.last() {
            if !building_segment.segment().dumping() {
                let building_segment = building_segment.segment().clone();
                self.dump_building_segment(building_segment)
            }
        }
        let base_docid = self
            .building_segments
            .last()
            .map(|segment| segment.meta().end_docid())
            .or(self
                .persistent_segments
                .last()
                .map(|segment| segment.meta().end_docid()))
            .unwrap_or_default();
        let doc_count = (DocId::MAX - base_docid) as usize;
        let meta = SegmentMeta::new(building_segment.segment_id().clone(), base_docid, doc_count);
        let building_segment = BuildingSegment::new(meta, building_segment);
        self.building_segments.push(building_segment);
    }

    pub fn dump_building_segment(&mut self, building_segment_data: Arc<BuildingSegmentData>) {
        assert!(Arc::ptr_eq(
            self.building_segments.last().unwrap().segment(),
            &building_segment_data
        ));
        building_segment_data.set_dumping_start();
        let building_segment = self.building_segments.last_mut().unwrap();
        building_segment
            .meta_mut()
            .set_doc_count(building_segment_data.doc_count());
        let segment_directory = self
            .directory
            .join("segments")
            .join(building_segment_data.segment_id().as_str());
        let column_directory = segment_directory.join("column");
        fs::create_dir_all(&column_directory).unwrap();
        let column_serializer_factory = ColumnSerializerFactory::default();
        for field in self.schema.columns() {
            let column_data = building_segment_data
                .column_data()
                .column_data(field.name())
                .unwrap()
                .clone();
            let column_serializer = column_serializer_factory.create(field, column_data);
            column_serializer.serialize(&column_directory);
        }

        let index_directory = segment_directory.join("index");
        fs::create_dir_all(&index_directory).unwrap();
        let index_serializer_factory = IndexSerializerFactory::default();
        for index in self.schema.indexes() {
            let index_data = building_segment_data
                .index_data()
                .index_data(index.name())
                .unwrap()
                .clone();
            let index_serializer = index_serializer_factory.create(index, index_data);
            index_serializer.serialize(&index_directory);
        }

        if !building_segment_data.deletemap().is_empty() {
            let path = segment_directory.join("deletionmap");
            building_segment_data.deletemap().save(path);
        }

        let meta = SegmentMetaData::new(building_segment_data.doc_count());
        meta.save(segment_directory.join("meta.json"));
        let mut version = self.version.next_version();
        version.add_segment(building_segment_data.segment_id().clone());
        version.save(&self.directory);

        self.remove_building_segment(building_segment_data);
        self.reload();

        if version.segments().len() > 1 {
            let mut version_merged = version.next_version();
            self.merge_segments(&mut version_merged);
        }
    }

    pub fn remove_building_segment(&mut self, building_segment: Arc<BuildingSegmentData>) {
        self.building_segments
            .retain(|segment| !Arc::ptr_eq(segment.segment(), &building_segment));
    }

    pub fn merge_segments(&mut self, version: &mut Version) {
        let segment_merger = SegmentMerger::default();
        segment_merger.merge(&self.directory, &self.schema, version);

        self.reload();
    }

    pub fn persistent_segments(&self) -> impl Iterator<Item = &PersistentSegment> {
        self.persistent_segments.iter()
    }

    pub fn building_segments(&self) -> impl Iterator<Item = &BuildingSegment> {
        self.building_segments.iter()
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
