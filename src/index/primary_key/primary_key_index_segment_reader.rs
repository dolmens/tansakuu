use std::sync::Arc;

use crate::DocId;

use super::PrimaryKeyIndexSegmentData;

pub struct PrimaryKeyIndexSegmentReader {
    index_data: Arc<PrimaryKeyIndexSegmentData>,
}

impl PrimaryKeyIndexSegmentReader {
    pub fn new(index_data: Arc<PrimaryKeyIndexSegmentData>) -> Self {
        Self { index_data }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.index_data.lookup(key)
    }
}
