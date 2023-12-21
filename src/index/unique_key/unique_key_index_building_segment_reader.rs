use std::sync::Arc;

use crate::DocId;

use super::UniqueKeyIndexBuildingSegmentData;

pub struct UniqueKeyIndexBuildingSegmentReader {
    index_data: Arc<UniqueKeyIndexBuildingSegmentData>,
}

impl UniqueKeyIndexBuildingSegmentReader {
    pub fn new(index_data: Arc<UniqueKeyIndexBuildingSegmentData>) -> Self {
        Self { index_data }
    }

    pub fn lookup(&self, key: &str) -> DocId {
        self.index_data.lookup(key)
    }
}
