use std::sync::Arc;

use crate::index::SegmentPosting;

use super::TermIndexBuildingSegmentData;

pub struct TermIndexBuildingSegmentReader {
    index_data: Arc<TermIndexBuildingSegmentData>,
}

impl TermIndexBuildingSegmentReader {
    pub fn new(index_data: Arc<TermIndexBuildingSegmentData>) -> Self {
        Self { index_data }
    }

    pub fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let postings = self.index_data.postings.lock().unwrap();
        let docids = postings.get(tok).cloned().unwrap_or_default();
        SegmentPosting { docids }
    }
}
