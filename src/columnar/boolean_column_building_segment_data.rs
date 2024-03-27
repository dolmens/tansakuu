use crate::util::Bitset;

use super::ColumnBuildingSegmentData;

pub struct BooleanColumnBuildingSegmentData {
    pub nullable: bool,
    pub values: Bitset,
    pub nulls: Option<Bitset>,
}

impl ColumnBuildingSegmentData for BooleanColumnBuildingSegmentData {}
