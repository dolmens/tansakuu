use std::sync::Arc;

use crate::{table::SegmentMetaInfo, DocId};

use super::PrimaryKeyIndexSegmentData;

pub struct PrimaryKeyIndexSegmentReader {
    meta_info: SegmentMetaInfo,
    index_data: Arc<PrimaryKeyIndexSegmentData>,
}

impl PrimaryKeyIndexSegmentReader {
    pub fn new(meta_info: SegmentMetaInfo, index_data: Arc<PrimaryKeyIndexSegmentData>) -> Self {
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
