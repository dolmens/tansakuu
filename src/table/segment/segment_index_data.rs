use std::{collections::HashMap, sync::Arc};

use crate::index::IndexSegmentData;

pub struct SegmentIndexData {
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
}

impl SegmentIndexData {
    pub fn new(indexes: HashMap<String, Arc<dyn IndexSegmentData>>) -> Self {
        Self { indexes }
    }

    pub fn index(&self, name: &str) -> Option<&Arc<dyn IndexSegmentData>> {
        self.indexes.get(name)
    }
}
