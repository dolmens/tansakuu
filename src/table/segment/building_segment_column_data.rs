use std::{collections::HashMap, sync::Arc};

use crate::{column::ColumnSegmentData, table::TableColumnWriter};

pub struct BuildingSegmentColumnData {
    columns: HashMap<String, Arc<dyn ColumnSegmentData>>,
}

impl BuildingSegmentColumnData {
    pub fn new(column_writer: &TableColumnWriter) -> Self {
        let mut indexes = HashMap::new();
        for (name, writer) in column_writer.column_writers() {
            indexes.insert(name.to_string(), writer.column_data());
        }

        Self { columns: indexes }
    }

    pub fn column_data(&self, name: &str) -> Option<&Arc<dyn ColumnSegmentData>> {
        self.columns.get(name)
    }
}
