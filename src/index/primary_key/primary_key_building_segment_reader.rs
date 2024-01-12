use std::sync::Arc;

use crate::{table::SegmentMeta, DocId};

use super::PrimaryKeyBuildingSegmentData;

pub struct PrimaryKeyBuildingSegmentReader {
    meta: SegmentMeta,
    index_data: Arc<PrimaryKeyBuildingSegmentData>,
}

impl PrimaryKeyBuildingSegmentReader {
    pub fn new(meta: SegmentMeta, index_data: Arc<PrimaryKeyBuildingSegmentData>) -> Self {
        Self { meta, index_data }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.index_data
            .lookup(key)
            .map(|docid| docid + self.meta.base_docid())
    }
}
