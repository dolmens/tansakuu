use std::sync::Arc;

use crate::{table::SegmentMeta, util::layered_hashmap::LayeredHashMap, DocId};

use super::PrimaryKeyBuildingSegmentData;

pub struct PrimaryKeyBuildingSegmentReader {
    meta: SegmentMeta,
    keys: LayeredHashMap<String, DocId>,
}

impl PrimaryKeyBuildingSegmentReader {
    pub fn new(meta: SegmentMeta, index_data: Arc<PrimaryKeyBuildingSegmentData>) -> Self {
        Self {
            meta,
            keys: index_data.keys.clone(),
        }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.keys
            .get(key)
            .cloned()
            .map(|docid| docid + self.meta.base_docid())
    }
}
