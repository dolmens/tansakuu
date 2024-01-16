use std::collections::{hash_map::RandomState, HashMap};

use crate::{
    index::IndexSegmentData,
    postings::BuildingPostingList,
    util::{FixedCapacityPolicy, LayeredHashMap},
    DocId,
};

pub struct InvertedIndexBuildingSegmentData {
    pub postings: LayeredHashMap<String, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentData {
    pub fn new() -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let postings = LayeredHashMap::with_initial_capacity(1024, hasher_builder, capacity_policy);

        Self { postings }
    }

    pub fn postings(&self) -> HashMap<String, Vec<DocId>> {
        unimplemented!()
        // let mut ps = HashMap::new();
        // for (key, building_doc_list) in self.postings.iter() {
        //     let doc_list_reader = BuildingPostingReader::open(building_doc_list);
        //     let docids: Vec<_> = doc_list_reader
        //         .into_iter()
        //         .map(|(docid, _)| docid)
        //         .collect();
        //     ps.insert(key.to_string(), docids);
        // }
        // ps
    }
}

impl IndexSegmentData for InvertedIndexBuildingSegmentData {}
