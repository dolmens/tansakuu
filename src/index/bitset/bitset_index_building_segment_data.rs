use crate::{index::IndexSegmentData, schema::IndexRef, util::Bitset};

pub struct BitsetIndexBuildingSegmentData {
    pub values: Bitset,
    pub nulls: Option<Bitset>,
    pub index: IndexRef,
}

impl BitsetIndexBuildingSegmentData {
    pub fn new(index: IndexRef, values: Bitset, nulls: Option<Bitset>) -> Self {
        Self {
            values,
            nulls,
            index,
        }
    }
}

impl IndexSegmentData for BitsetIndexBuildingSegmentData {
    fn collect_stat(&self, _segment_stat: &mut crate::table::SegmentStat) {
        // TODO:
    }
}
