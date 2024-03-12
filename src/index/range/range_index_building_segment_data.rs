use crate::{
    index::IndexSegmentData, postings::BuildingPostingList, util::layered_hashmap::LayeredHashMap,
};

pub struct RangeIndexBuildingSegmentData {
    pub bottom_postings: LayeredHashMap<u64, BuildingPostingList>,
    pub higher_postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl IndexSegmentData for RangeIndexBuildingSegmentData {
    fn collect_stat(&self, _segment_stat: &mut crate::table::SegmentStat) {}
}
