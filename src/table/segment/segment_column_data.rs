use std::{collections::HashMap, sync::Arc};

use crate::column::ColumnSegmentData;

pub struct SegmentColumnData {
    columns: HashMap<String, Arc<dyn ColumnSegmentData>>,
}

impl SegmentColumnData {
    pub fn new(columns: HashMap<String, Arc<dyn ColumnSegmentData>>) -> Self {
        Self { columns }
    }

    pub fn column(&self, name: &str) -> Option<&Arc<dyn ColumnSegmentData>> {
        self.columns.get(name)
    }
}
