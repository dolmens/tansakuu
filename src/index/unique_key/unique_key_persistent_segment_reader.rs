use std::sync::Arc;

use crate::{table::SegmentMeta, DocId};

use super::UniqueKeyPersistentSegmentData;

pub struct UniqueKeyPersistentSegmentReader {
    meta: SegmentMeta,
    index_data: Arc<UniqueKeyPersistentSegmentData>,
}

impl UniqueKeyPersistentSegmentReader {
    pub fn new(meta: SegmentMeta, index_data: Arc<UniqueKeyPersistentSegmentData>) -> Self {
        Self { meta, index_data }
    }

    pub fn get_by_hashkey(&self, hashkey: u64) -> Option<DocId> {
        self.index_data
            .get_by_hashkey(hashkey)
            .map(|docid| docid + self.meta.base_docid())
    }
}
