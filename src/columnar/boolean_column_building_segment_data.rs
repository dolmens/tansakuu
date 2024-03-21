use crate::util::ExpandableBitset;

use super::ColumnBuildingSegmentData;

pub struct BooleanColumnBuildingSegmentData {
    pub nullable: bool,
    pub values: ExpandableBitset,
    pub nulls: ExpandableBitset,
}

impl ColumnBuildingSegmentData for BooleanColumnBuildingSegmentData {}
