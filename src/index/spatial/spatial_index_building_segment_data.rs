use crate::{
    index::IndexSegmentData, postings::BuildingPostingList, schema::IndexRef,
    util::layered_hashmap::LayeredHashMap,
};

pub struct SpatialIndexBuildingSegmentData {
    pub index: IndexRef,
    pub postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl SpatialIndexBuildingSegmentData {
    pub fn new(index: IndexRef, postings: LayeredHashMap<u64, BuildingPostingList>) -> Self {
        Self { index, postings }
    }

    pub fn index(&self) -> &IndexRef {
        &self.index
    }
}

impl IndexSegmentData for SpatialIndexBuildingSegmentData {
    fn collect_stat(&self, _segment_stat: &mut crate::table::SegmentStat) {
        // TODO:
    }
}
