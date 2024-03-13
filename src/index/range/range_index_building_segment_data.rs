use crate::{
    index::IndexSegmentData, postings::BuildingPostingList, schema::IndexRef,
    util::layered_hashmap::LayeredHashMap,
};

pub struct RangeIndexBuildingSegmentData {
    pub index: IndexRef,
    pub bottom_postings: LayeredHashMap<u64, BuildingPostingList>,
    pub higher_postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl RangeIndexBuildingSegmentData {
    pub fn new(
        index: IndexRef,
        bottom_postings: LayeredHashMap<u64, BuildingPostingList>,
        higher_postings: LayeredHashMap<u64, BuildingPostingList>,
    ) -> Self {
        Self {
            index,
            bottom_postings,
            higher_postings,
        }
    }

    pub fn index(&self) -> &IndexRef {
        &self.index
    }
}

impl IndexSegmentData for RangeIndexBuildingSegmentData {
    fn collect_stat(&self, _segment_stat: &mut crate::table::SegmentStat) {}
}
