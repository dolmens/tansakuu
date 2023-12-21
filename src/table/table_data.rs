use std::{collections::VecDeque, sync::Arc};

use crate::schema::SchemaRef;

use super::{
    segment::{BuildingSegment, Segment},
    TableSettingsRef,
};

#[derive(Clone)]
pub struct TableData {
    building_segments: Vec<Arc<BuildingSegment>>,
    dumping_segments: VecDeque<Arc<BuildingSegment>>,
    segments: Vec<Arc<Segment>>,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableDataRef = Arc<TableData>;

impl TableData {
    pub fn new(schema: SchemaRef, settings: TableSettingsRef) -> Self {
        Self {
            building_segments: vec![],
            dumping_segments: VecDeque::new(),
            segments: vec![],
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
        self.dumping_segments.push_back(building_segment);
    }

    pub fn building_segments(&self) -> impl Iterator<Item = &Arc<BuildingSegment>> {
        self.building_segments.iter()
    }

    pub fn dumping_segments(&self) -> impl Iterator<Item = &Arc<BuildingSegment>> {
        self.dumping_segments.iter()
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn settings(&self) -> &TableSettingsRef {
        &self.settings
    }
}
