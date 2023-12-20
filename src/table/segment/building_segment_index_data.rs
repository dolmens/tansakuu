use std::{collections::HashMap, sync::Arc};

use crate::index::IndexSegmentData;

pub struct BuildingSegmentIndexData {
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
}

impl BuildingSegmentIndexData {
    pub fn new(indexes: HashMap<String, Arc<dyn IndexSegmentData>>) -> Self {
        Self { indexes }
    }

    pub fn index_data(&self, name: &str) -> Option<&Arc<dyn IndexSegmentData>> {
        self.indexes.get(name)
    }
}
