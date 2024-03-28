use crate::{index::IndexSegmentData, util::ImmutableBitset8};

pub struct BitsetIndexPersistentSegmentData {
    pub values: Option<ImmutableBitset8>,
    pub nulls: Option<ImmutableBitset8>,
}

impl IndexSegmentData for BitsetIndexPersistentSegmentData {}
