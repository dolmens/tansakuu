use crate::{
    index::IndexSegmentData, postings::BuildingPostingList, schema::IndexRef,
    util::layered_hashmap::LayeredHashMap,
};

pub struct InvertedIndexBuildingSegmentData {
    pub index: IndexRef,
    pub postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentData {
    pub fn new(index: IndexRef, postings: LayeredHashMap<u64, BuildingPostingList>) -> Self {
        Self { index, postings }
    }
}

impl IndexSegmentData for InvertedIndexBuildingSegmentData {
    fn collect_stat(&self, segment_stat: &mut crate::table::SegmentStat) {
        segment_stat
            .index_term_count
            .insert(self.index.name().to_string(), self.postings.len());
    }
}
