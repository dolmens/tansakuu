use crate::index::{inverted_index::PersistentPostingData, IndexSegmentData};

pub struct RangeIndexPersistentSegmentData {
    pub bottom_posting_data: PersistentPostingData,
    pub higher_posting_data: PersistentPostingData,
}

impl IndexSegmentData for RangeIndexPersistentSegmentData {}
