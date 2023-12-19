use std::{collections::HashMap, sync::Arc};

use crate::index::IndexSegmentData;

#[derive(Clone)]
pub struct BuildingSegment {
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
}

impl BuildingSegment {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    pub fn add_index_data(&mut self, index_name: String, data: Arc<dyn IndexSegmentData>) {
        self.indexes.insert(index_name, data);
    }

    pub fn index_data(&self, index_name: &str) -> &Arc<dyn IndexSegmentData> {
        self.indexes.get(index_name).unwrap()
    }
}
