use std::{collections::HashMap, sync::Arc};

use crate::index::IndexSegmentData;

use super::SegmentStat;

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

    pub fn collect_segment_stat(&self, segment_stat: &mut SegmentStat) {
        for (_, index) in &self.indexes {
            index.collect_stat(segment_stat);
        }
    }
}
