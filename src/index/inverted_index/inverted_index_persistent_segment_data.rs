use crate::index::IndexSegmentData;

use super::PersistentPostingData;

pub struct InvertedIndexPersistentSegmentData {
    pub posting_data: PersistentPostingData,
}

impl InvertedIndexPersistentSegmentData {
    pub fn new(posting_data: PersistentPostingData) -> Self {
        Self { posting_data }
    }
}

impl IndexSegmentData for InvertedIndexPersistentSegmentData {}
