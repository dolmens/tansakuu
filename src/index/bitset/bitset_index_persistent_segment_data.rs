use crate::{index::IndexSegmentData, util::ImmutableBitset};

pub struct BitsetIndexPersistentSegmentData {
    pub values: Option<ImmutableBitset>,
    pub nulls: Option<ImmutableBitset>,
}

impl IndexSegmentData for BitsetIndexPersistentSegmentData {}
