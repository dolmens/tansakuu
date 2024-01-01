use std::sync::Arc;

use crate::{table::SegmentMeta, DocId};

use super::PrimaryKeyIndexPersistentSegmentData;

pub struct PrimaryKeyIndexPersistentSegmentReader {
    meta: SegmentMeta,
    index_data: Arc<PrimaryKeyIndexPersistentSegmentData>,
}

impl PrimaryKeyIndexPersistentSegmentReader {
    pub fn new(meta: SegmentMeta, index_data: Arc<PrimaryKeyIndexPersistentSegmentData>) -> Self {
        Self { meta, index_data }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.index_data
            .lookup(key)
            .map(|docid| docid + self.meta.base_docid())
    }
}
