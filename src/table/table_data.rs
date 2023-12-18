use std::sync::Arc;

use crate::schema::SchemaRef;

use super::{segment::BuildingSegment, TableSettingsRef};

pub struct TableData {
    building_segments: Vec<BuildingSegment>,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableDataRef = Arc<TableData>;

impl Clone for TableData {
    fn clone(&self) -> Self {
        Self {
            building_segments: vec![],
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

    pub fn building_segments(&self) -> &[BuildingSegment] {
        &self.building_segments
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn settings(&self) -> &TableSettingsRef {
        &self.settings
    }
}
