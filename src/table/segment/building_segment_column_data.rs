use std::{collections::HashMap, sync::Arc};

use crate::columnar::ColumnSegmentData;

pub struct BuildingSegmentColumnData {
    columns: HashMap<String, Arc<dyn ColumnSegmentData>>,
}

impl BuildingSegmentColumnData {
    pub fn new(columns: HashMap<String, Arc<dyn ColumnSegmentData>>) -> Self {
        Self { columns }
    }

    pub fn column_data(&self, name: &str) -> Option<&Arc<dyn ColumnSegmentData>> {
        self.columns.get(name)
    }
}
