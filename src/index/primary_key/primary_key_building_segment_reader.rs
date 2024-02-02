use std::sync::Arc;

use crate::{table::SegmentMeta, util::layered_hashmap::LayeredHashMap, DocId};

use super::PrimaryKeyBuildingSegmentData;

pub struct PrimaryKeyBuildingSegmentReader {
    meta: SegmentMeta,
    keys: LayeredHashMap<u64, DocId>,
}

impl PrimaryKeyBuildingSegmentReader {
    pub fn new(meta: SegmentMeta, index_data: Arc<PrimaryKeyBuildingSegmentData>) -> Self {
        Self {
            meta,
            keys: index_data.keys.clone(),
        }
    }

    pub fn get_by_hashkey(&self, hashkey: u64) -> Option<DocId> {
        self.keys
            .get(&hashkey)
            .cloned()
            .map(|docid| docid + self.meta.base_docid())
    }
}
