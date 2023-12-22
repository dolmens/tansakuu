use std::{fs, path::PathBuf, sync::Arc};

use crate::schema::SchemaRef;

use super::{
    segment::{BuildingSegment, Segment},
    TableSettingsRef,
};

#[derive(Clone)]
pub struct TableData {
    building_segments: Vec<Arc<BuildingSegment>>,
    segments: Vec<Arc<Segment>>,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableDataRef = Arc<TableData>;

impl TableData {
    pub fn new(directory: PathBuf, schema: SchemaRef, settings: TableSettingsRef) -> Self {
        let mut segments = vec![];
        let segments_directory = directory.join("segments");
        if let Ok(entries) = fs::read_dir(segments_directory) {
            for entry in entries {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    segments.push(Arc::new(Segment::new(&schema, path)));
                }
            }
        }

        Self {
            building_segments: vec![],
            segments,
            schema,
            settings,
        }
    }

    pub fn add_building_segment(&mut self, building_segment: Arc<BuildingSegment>) {
        self.building_segments.push(building_segment);
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

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn settings(&self) -> &TableSettingsRef {
        &self.settings
    }
}
