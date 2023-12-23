use std::{collections::HashSet, path::PathBuf, sync::Arc};

use crate::schema::SchemaRef;

use super::{
    segment::{BuildingSegment, Segment},
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

impl TableData {
    pub fn new(directory: PathBuf, schema: SchemaRef, settings: TableSettingsRef) -> Self {
        let version = Version::load_lastest(&directory);
        let mut segments = vec![];
        for segment_name in version.segments() {
            let segment_directory = directory.join("segments").join(segment_name);
            segments.push(Arc::new(Segment::new(
                segment_name.clone(),
                &schema,
                segment_directory,
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
                .retain(|segment| new_segments_set.contains(segment.segment_name()));
            let current_segments_set: HashSet<_> = self
                .segments()
                .map(|segment| segment.segment_name().to_string())
                .collect();
            for segment_name in version.segments() {
                if !current_segments_set.contains(segment_name) {
                    let segment_directory = self.directory.join("segments").join(segment_name);
                    self.segments.push(Arc::new(Segment::new(
                        segment_name.clone(),
                        &self.schema,
                        segment_directory,
                    )));
                }
            }
        }
    }

    pub fn add_building_segment(&mut self, building_segment: Arc<BuildingSegment>) {
        self.building_segments.push(building_segment);
    }

    pub fn remove_building_segment(&mut self, building_segment: &Arc<BuildingSegment>) {
        self.building_segments
            .retain(|segment| !Arc::ptr_eq(segment, &building_segment));
    }

    pub fn dump_segment(&mut self, building_segment: Arc<BuildingSegment>) {
        if let Some(pos) = self
            .building_segments
            .iter()
            .position(|x| Arc::ptr_eq(x, &building_segment))
        {
            self.building_segments.remove(pos);
        }
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
