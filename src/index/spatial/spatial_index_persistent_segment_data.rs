use crate::index::{inverted_index::PersistentPostingData, IndexSegmentData};

pub struct SpatialIndexPersistentSegmentData {
    pub posting_data: PersistentPostingData,
}

impl IndexSegmentData for SpatialIndexPersistentSegmentData {}
