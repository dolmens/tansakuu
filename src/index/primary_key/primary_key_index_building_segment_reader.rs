use std::sync::Arc;

use crate::DocId;

use super::PrimaryKeyIndexBuildingSegmentData;

pub struct PrimaryKeyIndexBuildingSegmentReader {
    index_data: Arc<PrimaryKeyIndexBuildingSegmentData>,
}

impl PrimaryKeyIndexBuildingSegmentReader {
    pub fn new(index_data: Arc<PrimaryKeyIndexBuildingSegmentData>) -> Self {
        Self { index_data }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.index_data.lookup(key)
    }
}
