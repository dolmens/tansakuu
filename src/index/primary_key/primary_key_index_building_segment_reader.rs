use std::sync::Arc;

use crate::{table::SegmentMetaInfo, DocId};

use super::PrimaryKeyIndexBuildingSegmentData;

pub struct PrimaryKeyIndexBuildingSegmentReader {
    meta_info: SegmentMetaInfo,
    index_data: Arc<PrimaryKeyIndexBuildingSegmentData>,
}

impl PrimaryKeyIndexBuildingSegmentReader {
    pub fn new(
        meta_info: SegmentMetaInfo,
        index_data: Arc<PrimaryKeyIndexBuildingSegmentData>,
    ) -> Self {
        Self {
            meta_info,
            index_data,
        }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.index_data
            .lookup(key)
            .map(|docid| docid + self.meta_info.base_docid())
    }
}
