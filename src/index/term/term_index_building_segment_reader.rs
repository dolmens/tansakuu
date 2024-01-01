use std::sync::Arc;

use crate::{index::SegmentPosting, DocId};

use super::TermIndexBuildingSegmentData;

pub struct TermIndexBuildingSegmentReader {
    base_docid: DocId,
    index_data: Arc<TermIndexBuildingSegmentData>,
}

impl TermIndexBuildingSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<TermIndexBuildingSegmentData>) -> Self {
        Self {
            base_docid,
            index_data,
        }
    }

    pub fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let postings = self.index_data.postings.lock().unwrap();
        let docids = postings.get(tok).cloned().unwrap_or_default();
        SegmentPosting::new(self.base_docid, docids)
    }
}
