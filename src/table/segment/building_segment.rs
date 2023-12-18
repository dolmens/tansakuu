use std::{collections::HashMap, sync::Arc};

use crate::index::IndexSegmentData;

pub struct BuildingSegment {
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
}

impl BuildingSegment {
    pub fn index_data(&self, index_name: &str) -> &Arc<dyn IndexSegmentData> {
        self.indexes.get(index_name).unwrap()
    }
}
