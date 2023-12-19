use std::sync::Arc;

use crate::index::{IndexSegmentReader, SegmentPosting};

use super::TermIndexSegmentData;

pub struct TermIndexSegmentReader {
    index_data: Arc<TermIndexSegmentData>,
}

impl TermIndexSegmentReader {
    pub fn new(index_data: Arc<TermIndexSegmentData>) -> Self {
        Self { index_data }
    }
}

impl IndexSegmentReader for TermIndexSegmentReader {
    fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let postings = self.index_data.postings.lock().unwrap();
        let docids = postings.get(tok).cloned().unwrap_or_default();
        SegmentPosting { docids }
    }
}
