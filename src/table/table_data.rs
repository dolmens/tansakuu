use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};

use crate::{
    column::ColumnSerializerFactory, index::IndexSerializerFactory, schema::SchemaRef,
    table::segment::SegmentMeta, DocId,
};

use super::{
    segment::{BuildingSegment, Segment, SegmentMerger},
    SegmentId, TableSettingsRef, Version,
};

#[derive(Clone)]
pub struct TableData {
    building_segments: Vec<BuildingSegmentInfo>,
    segments: Vec<SegmentInfo>,
    version: Version,
    directory: PathBuf,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableDataRef = Arc<TableData>;

#[derive(Clone)]
pub struct BuildingSegmentInfo {
    meta_info: SegmentMetaInfo,
    segment: Arc<BuildingSegment>,
}

#[derive(Clone)]
pub struct SegmentInfo {
    meta_info: SegmentMetaInfo,
    segment: Arc<Segment>,
}

#[derive(Clone)]
pub struct SegmentMetaInfo {
    segment_id: SegmentId,
    base_docid: DocId,
    doc_count: usize,
}

impl TableData {
    pub fn open(directory: PathBuf, schema: SchemaRef, settings: TableSettingsRef) -> Self {
        let version = Version::load_lastest(&directory);
        let segment_directory = directory.join("segments");
        let mut segments = vec![];
        let mut base_docid = 0;
        for segment_id in version.segments() {
            let segment = Arc::new(Segment::open(
                segment_id.clone(),
                &schema,
                &segment_directory,
            ));
            let doc_count = segment.doc_count();
            let meta_info =
                SegmentMetaInfo::new(segment.segment_id().clone(), base_docid, doc_count);
            segments.push(SegmentInfo::new(meta_info, segment));
            base_docid += doc_count as DocId;
        }

        Self {
            building_segments: vec![],
            segments,
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
                .segments
                .iter()
                .map(|segment| {
                    (
                        segment.segment.segment_id().clone(),
                        segment.segment.clone(),
                    )
                })
                .collect();
            let mut segments = vec![];
            let mut base_docid = 0;
            for segment_id in version.segments() {
                let segment = if let Some(segment) = current_segments_map.get(segment_id) {
                    segment.clone()
                } else {
                    Arc::new(Segment::open(
                        segment_id.clone(),
                        &self.schema,
                        &segment_directory,
                    ))
                };

                let meta_info =
                    SegmentMetaInfo::new(segment_id.clone(), base_docid, segment.doc_count());
                segments.push(SegmentInfo::new(meta_info, segment.clone()));
                base_docid += segment.doc_count() as DocId;
            }
            std::mem::swap(&mut self.segments, &mut segments);
            for building_segment in &mut self.building_segments {
                building_segment.meta_info.base_docid = base_docid;
                base_docid += building_segment.meta_info.doc_count as DocId;
            }
            self.version = version;
        }
    }

    pub fn add_building_segment(&mut self, building_segment: Arc<BuildingSegment>) {
        if let Some(building_segment) = self.building_segments.last() {
            if !building_segment.segment.dumping() {
                let building_segment = building_segment.segment.clone();
                self.dump_building_segment(building_segment)
            }
        }
        let base_docid = self
            .building_segments
            .last()
            .map(|segment| segment.meta_info.end_docid())
            .or(self
                .segments
                .last()
                .map(|segment| segment.meta_info.end_docid()))
            .unwrap_or_default();
        let doc_count = (DocId::MAX - base_docid) as usize;
        let meta_info =
            SegmentMetaInfo::new(building_segment.segment_id().clone(), base_docid, doc_count);
        let building_segment = BuildingSegmentInfo::new(meta_info, building_segment);
        self.building_segments.push(building_segment);
    }

    pub fn dump_building_segment(&mut self, building_segment: Arc<BuildingSegment>) {
        assert!(Arc::ptr_eq(
            self.building_segments.last().unwrap().segment(),
            &building_segment
        ));
        building_segment.set_dumping_start();
        let building_segment_info = self.building_segments.last_mut().unwrap();
        building_segment_info.meta_info.doc_count = building_segment.doc_count();
        let segment_directory = self
            .directory
            .join("segments")
            .join(building_segment.segment_id().as_str());
        let column_directory = segment_directory.join("column");
        fs::create_dir_all(&column_directory).unwrap();
        let column_serializer_factory = ColumnSerializerFactory::default();
        for field in self.schema.columns() {
            let column_data = building_segment
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
            let index_data = building_segment
                .index_data()
                .index_data(index.name())
                .unwrap()
                .clone();
            let index_serializer = index_serializer_factory.create(index, index_data);
            index_serializer.serialize(&index_directory);
        }

        if !building_segment.deletemap().is_empty() {
            let path = segment_directory.join("deletionmap");
            building_segment.deletemap().save(path);
        }

        let meta = SegmentMeta::new(building_segment.doc_count());
        meta.save(segment_directory.join("meta.json"));
        let mut version = self.version.next_version();
        version.add_segment(building_segment.segment_id().clone());
        version.save(&self.directory);

        self.remove_building_segment(building_segment);
        self.reload();

        if version.segments().len() > 1 {
            let mut version_merged = version.next_version();
            self.merge_segments(&mut version_merged);
        }
    }

    pub fn remove_building_segment(&mut self, building_segment: Arc<BuildingSegment>) {
        self.building_segments
            .retain(|segment| !Arc::ptr_eq(segment.segment(), &building_segment));
    }

    pub fn merge_segments(&mut self, version: &mut Version) {
        let segment_merger = SegmentMerger::default();
        segment_merger.merge(&self.directory, &self.schema, version);

        self.reload();
    }

    pub fn segments(&self) -> impl Iterator<Item = &SegmentInfo> {
        self.segments.iter()
    }

    pub fn building_segments(&self) -> impl Iterator<Item = &BuildingSegmentInfo> {
        self.building_segments.iter()
    }

    pub fn segment_of_docid(&self, docid: DocId) -> Option<(&SegmentId, DocId)> {
        self.segments
            .iter()
            .find(|segment| docid < segment.meta_info.end_docid())
            .map(|segment| {
                (
                    segment.meta_info.segment_id(),
                    docid - segment.meta_info.base_docid,
                )
            })
            .or(self
                .building_segments
                .iter()
                .find(|segment| docid < segment.meta_info.end_docid())
                .map(|segment| {
                    (
                        segment.meta_info.segment_id(),
                        docid - segment.meta_info.base_docid,
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

impl BuildingSegmentInfo {
    pub fn new(meta_info: SegmentMetaInfo, segment: Arc<BuildingSegment>) -> Self {
        Self { meta_info, segment }
    }

    pub fn meta_info(&self) -> &SegmentMetaInfo {
        &self.meta_info
    }

    pub fn segment(&self) -> &Arc<BuildingSegment> {
        &self.segment
    }

    pub fn segment_id(&self) -> &SegmentId {
        self.segment.segment_id()
    }
}

impl SegmentInfo {
    pub fn new(meta_info: SegmentMetaInfo, segment: Arc<Segment>) -> Self {
        Self { meta_info, segment }
    }

    pub fn meta_info(&self) -> &SegmentMetaInfo {
        &self.meta_info
    }

    pub fn segment(&self) -> &Arc<Segment> {
        &self.segment
    }
}

impl SegmentMetaInfo {
    pub fn new(segment_id: SegmentId, base_docid: DocId, doc_count: usize) -> Self {
        Self {
            segment_id,
            base_docid,
            doc_count,
        }
    }

    pub fn segment_id(&self) -> &SegmentId {
        &self.segment_id
    }

    pub fn base_docid(&self) -> DocId {
        self.base_docid
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    pub fn end_docid(&self) -> DocId {
        self.base_docid + (self.doc_count as DocId)
    }

    pub fn inner_docid(&self, docid: DocId) -> DocId {
        docid - self.base_docid
    }
}
