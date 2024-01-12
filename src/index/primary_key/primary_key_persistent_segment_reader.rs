use std::sync::Arc;

use crate::{table::SegmentMeta, DocId};

use super::PrimaryKeyPersistentSegmentData;

pub struct PrimaryKeyPersistentSegmentReader {
    meta: SegmentMeta,
    index_data: Arc<PrimaryKeyPersistentSegmentData>,
}

impl PrimaryKeyPersistentSegmentReader {
    pub fn new(meta: SegmentMeta, index_data: Arc<PrimaryKeyPersistentSegmentData>) -> Self {
        Self { meta, index_data }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.index_data
            .lookup(key)
            .map(|docid| docid + self.meta.base_docid())
    }
}
