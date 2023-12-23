use std::sync::Arc;

use crate::index::SegmentPosting;

use super::TermIndexSegmentData;

pub struct TermIndexSegmentReader {
    index_data: Arc<TermIndexSegmentData>,
}

impl TermIndexSegmentReader {
    pub fn new(index_data: Arc<TermIndexSegmentData>) -> Self {
        Self { index_data }
    }

    pub fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let docids = self
            .index_data
            .postings
            .get(tok)
            .cloned()
            .unwrap_or_default();
        SegmentPosting::new(0, docids)
    }
}
