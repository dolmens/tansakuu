use crate::{
    index::IndexSegmentData, postings::BuildingPostingList, util::layered_hashmap::LayeredHashMap,
};

pub struct InvertedIndexBuildingSegmentData {
    pub postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentData {
    pub fn new(postings: LayeredHashMap<u64, BuildingPostingList>) -> Self {
        Self { postings }
    }
}

impl IndexSegmentData for InvertedIndexBuildingSegmentData {}
