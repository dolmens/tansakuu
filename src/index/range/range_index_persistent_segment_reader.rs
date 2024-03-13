use std::sync::Arc;

use crate::{index::inverted_index::SegmentMultiPosting, DocId};

use super::range_index_persistent_segment_data::RangeIndexPersistentSegmentData;

pub struct RangeIndexPersistentSegmentReader {
    base_docid: DocId,
    index_data: Arc<RangeIndexPersistentSegmentData>,
}

impl RangeIndexPersistentSegmentReader {
    pub fn lookup(
        &self,
        bottom_keys: &[u64],
        higher_keys: &[u64],
    ) -> Option<SegmentMultiPosting<'_>> {
        unimplemented!()
    }
}
