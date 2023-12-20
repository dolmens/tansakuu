use std::{collections::HashMap, sync::Arc};

use crate::{index::IndexSegmentData, table::TableIndexWriter};

pub struct BuildingSegmentIndexData {
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
}

impl BuildingSegmentIndexData {
    pub fn new(index_writer: &TableIndexWriter) -> Self {
        let mut indexes = HashMap::new();
        for (name, writer) in index_writer.index_writers() {
            indexes.insert(name.to_string(), writer.index_data());
        }

        Self { indexes }
    }

    pub fn index_data(&self, name: &str) -> Option<&Arc<dyn IndexSegmentData>> {
        self.indexes.get(name)
    }
}
