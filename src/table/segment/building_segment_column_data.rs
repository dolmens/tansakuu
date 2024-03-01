use std::{collections::HashMap, sync::Arc};

use crate::columnar::ColumnBuildingSegmentData;

pub struct BuildingSegmentColumnData {
    columns: HashMap<String, Arc<dyn ColumnBuildingSegmentData>>,
}

impl BuildingSegmentColumnData {
    pub fn new(columns: HashMap<String, Arc<dyn ColumnBuildingSegmentData>>) -> Self {
        Self { columns }
    }

    pub fn column_data(&self, name: &str) -> Option<&Arc<dyn ColumnBuildingSegmentData>> {
        self.columns.get(name)
    }

    pub fn columns(&self) -> &HashMap<String, Arc<dyn ColumnBuildingSegmentData>> {
        &self.columns
    }
}
