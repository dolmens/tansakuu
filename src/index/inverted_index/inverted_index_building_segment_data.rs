use std::collections::hash_map::RandomState;

use crate::{
    index::IndexSegmentData,
    postings::BuildingPostingList,
    util::{FixedCapacityPolicy, LayeredHashMap},
};

pub struct InvertedIndexBuildingSegmentData {
    pub postings: LayeredHashMap<String, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentData {
    pub fn new() -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let postings = LayeredHashMap::with_capacity(1024, hasher_builder, capacity_policy);

        Self { postings }
    }
}

impl IndexSegmentData for InvertedIndexBuildingSegmentData {}
