use crate::{index::IndexSegmentData, postings::BuildingPostingList, util::LayeredHashMap};

pub struct InvertedIndexBuildingSegmentData {
    pub postings: LayeredHashMap<String, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentData {
    pub fn new(postings: LayeredHashMap<String, BuildingPostingList>) -> Self {
        Self { postings }
    }
}

impl IndexSegmentData for InvertedIndexBuildingSegmentData {}
