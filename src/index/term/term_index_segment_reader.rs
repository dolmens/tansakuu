use std::sync::Arc;

use crate::{index::SegmentPosting, DocId};

use super::TermIndexSegmentData;

pub struct TermIndexSegmentReader {
    base_docid: DocId,
    index_data: Arc<TermIndexSegmentData>,
}

impl TermIndexSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<TermIndexSegmentData>) -> Self {
        Self {
            base_docid,
            index_data,
        }
    }

    pub fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let docids = self
            .index_data
            .postings
            .get(tok)
            .cloned()
            .unwrap_or_default();
        SegmentPosting::new(self.base_docid, docids)
    }
}
