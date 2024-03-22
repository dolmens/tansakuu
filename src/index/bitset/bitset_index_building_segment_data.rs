use crate::{schema::IndexRef, util::ExpandableBitset, index::IndexSegmentData};

pub struct BitsetIndexBuildingSegmentData {
    pub nullable: bool,
    pub values: ExpandableBitset,
    pub nulls: Option<ExpandableBitset>,
    pub index: IndexRef,
}

impl BitsetIndexBuildingSegmentData {
    pub fn new(
        index: IndexRef,
        nullable: bool,
        values: ExpandableBitset,
        nulls: Option<ExpandableBitset>,
    ) -> Self {
        Self {
            nullable,
            values,
            nulls,
            index,
        }
    }
}

impl IndexSegmentData for BitsetIndexBuildingSegmentData {
    fn collect_stat(&self, segment_stat: &mut crate::table::SegmentStat) {
        // TODO:
    }
}
