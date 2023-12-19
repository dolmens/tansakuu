use std::sync::Arc;

use crate::DocId;

use super::UniqueKeyIndexSegmentData;

pub struct UniqueKeyIndexSegmentReader {
    index_data: Arc<UniqueKeyIndexSegmentData>,
}

impl UniqueKeyIndexSegmentReader {
    pub fn new(index_data: Arc<UniqueKeyIndexSegmentData>) -> Self {
        Self { index_data }
    }

    pub fn lookup(&self, key: &str) -> DocId {
        self.index_data.lookup(key)
    }
}
