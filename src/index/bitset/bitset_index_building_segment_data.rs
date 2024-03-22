use crate::{index::IndexSegmentData, schema::IndexRef, util::ExpandableBitset};

pub struct BitsetIndexBuildingSegmentData {
    pub values: ExpandableBitset,
    pub nulls: Option<ExpandableBitset>,
    pub index: IndexRef,
}

impl BitsetIndexBuildingSegmentData {
    pub fn new(index: IndexRef, values: ExpandableBitset, nulls: Option<ExpandableBitset>) -> Self {
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
