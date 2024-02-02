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

    pub fn get_by_hashkey(&self, hashkey: u64) -> Option<DocId> {
        self.index_data
            .get_by_hashkey(hashkey)
            .map(|docid| docid + self.meta.base_docid())
    }
}
