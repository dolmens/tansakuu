use std::sync::Arc;

use crate::schema::SchemaRef;

use super::{segment::BuildingSegment, TableSettingsRef};

pub struct TableData {
    building_segments: Vec<Arc<BuildingSegment>>,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableDataRef = Arc<TableData>;

impl Clone for TableData {
    fn clone(&self) -> Self {
        Self {
            building_segments: self.building_segments.clone(),
            schema: self.schema.clone(),
            settings: self.settings.clone(),
        }
    }
}

impl TableData {
    pub fn new(schema: SchemaRef, settings: TableSettingsRef) -> Self {
        Self {
            building_segments: vec![],
            schema,
            settings,
        }
    }

    pub fn add_building_segment(&mut self, building_segment: BuildingSegment) {
        self.building_segments.push(Arc::new(building_segment));
    }

    pub fn building_segments(&self) -> &[Arc<BuildingSegment>] {
        &self.building_segments
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn settings(&self) -> &TableSettingsRef {
        &self.settings
    }
}
