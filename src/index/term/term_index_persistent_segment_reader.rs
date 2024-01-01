use std::sync::Arc;

use crate::{index::SegmentPosting, DocId};

use super::TermIndexPersistentSegmentData;

pub struct TermIndexPersistentSegmentReader {
    base_docid: DocId,
    index_data: Arc<TermIndexPersistentSegmentData>,
}

impl TermIndexPersistentSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<TermIndexPersistentSegmentData>) -> Self {
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
