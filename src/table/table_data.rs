use std::{collections::HashSet, fs, path::PathBuf, sync::Arc};

use uuid::Uuid;

use crate::{
    column::ColumnSerializerFactory, index::IndexSerializerFactory, schema::SchemaRef,
    table::segment::SegmentMeta, DocId,
};

use super::{
    segment::{BuildingSegment, Segment, SegmentMerger},
    TableSettingsRef, Version,
};

#[derive(Clone)]
pub struct TableData {
    building_segments: Vec<Arc<BuildingSegment>>,
    segments: Vec<Arc<Segment>>,
    version: Version,
    directory: PathBuf,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableDataRef = Arc<TableData>;

#[derive(Default)]
pub struct SegmentDataSnapshot {
    pub base_docid: DocId,
    pub doc_count: usize,
}

#[derive(Default)]
pub struct TableDataSnapshot {
    pub segments: Vec<SegmentDataSnapshot>,
}

impl TableData {
    pub fn new(directory: PathBuf, schema: SchemaRef, settings: TableSettingsRef) -> Self {
        let version = Version::load_lastest(&directory);
        let mut segments = vec![];
        let segment_directory = directory.join("segments");
        for segment_name in version.segments() {
            segments.push(Arc::new(Segment::open(
                segment_name.clone(),
                &schema,
                &segment_directory,
            )));
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
            let new_segments_set: HashSet<_> =
                version.segments().iter().map(|s| s.as_str()).collect();
            self.segments
                .retain(|segment| new_segments_set.contains(segment.name()));
            let current_segments_set: HashSet<_> = self
                .segments()
                .map(|segment| segment.name().to_string())
                .collect();
            let segment_directory = self.directory.join("segments");
            for segment_name in version.segments() {
                if !current_segments_set.contains(segment_name) {
                    self.segments.push(Arc::new(Segment::open(
                        segment_name.clone(),
                        &self.schema,
                        &segment_directory,
                    )));
                }
            }
        }
        self.version = version;
    }

    pub fn add_building_segment(&mut self, building_segment: Arc<BuildingSegment>) {
        assert!(self.building_segments.is_empty());
        self.building_segments.push(building_segment);
    }

    pub fn dump_building_segment(&mut self, building_segment: Arc<BuildingSegment>) {
        assert!(Arc::ptr_eq(
            self.building_segments.last().unwrap(),
            &building_segment
        ));
        let segment_uuid = Uuid::new_v4();
        let segment_uuid_string = segment_uuid.as_simple().to_string();
        let segment_directory = self.directory.join("segments").join(&segment_uuid_string);

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

        let meta = SegmentMeta::new(building_segment.doc_count());
        meta.save(segment_directory.join("meta.json"));
        let mut version = self.version.new_version();
        version.add_segment(segment_uuid_string);
        version.save(&self.directory);

        self.remove_building_segment(&building_segment);
        self.reload();

        if version.segments().len() > 1 {
            let mut version_merged = version.new_version();
            self.merge_segments(&mut version_merged);
        }
    }

    pub fn dump_and_add_building_segment(
        &mut self,
        building_segment: Arc<BuildingSegment>,
        new_segment: Arc<BuildingSegment>,
    ) {
        self.dump_building_segment(building_segment);
        self.add_building_segment(new_segment);
    }

    pub fn remove_building_segment(&mut self, building_segment: &Arc<BuildingSegment>) {
        self.building_segments
            .retain(|segment| !Arc::ptr_eq(segment, &building_segment));
    }

    pub fn merge_segments(&mut self, version: &mut Version) {
        let segment_merger = SegmentMerger::default();
        segment_merger.merge(&self.directory, &self.schema, version);

        self.reload();
    }

    pub fn segments(&self) -> impl Iterator<Item = &Arc<Segment>> {
        self.segments.iter()
    }

    pub fn building_segments(&self) -> impl Iterator<Item = &Arc<BuildingSegment>> {
        self.building_segments.iter()
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

impl TableDataSnapshot {
    pub fn new() -> Self {
        Self::default()
    }
}
