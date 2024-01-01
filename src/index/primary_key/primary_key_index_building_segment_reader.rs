use std::sync::Arc;

use crate::{table::SegmentMeta, DocId};

use super::PrimaryKeyIndexBuildingSegmentData;

pub struct PrimaryKeyIndexBuildingSegmentReader {
    meta: SegmentMeta,
    index_data: Arc<PrimaryKeyIndexBuildingSegmentData>,
}

impl PrimaryKeyIndexBuildingSegmentReader {
    pub fn new(meta: SegmentMeta, index_data: Arc<PrimaryKeyIndexBuildingSegmentData>) -> Self {
        Self { meta, index_data }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.index_data
            .lookup(key)
            .map(|docid| docid + self.meta.base_docid())
    }
}
