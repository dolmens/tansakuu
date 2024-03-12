use std::sync::Arc;

use crate::{
    index::inverted_index::SegmentPosting, postings::BuildingPostingList,
    util::layered_hashmap::LayeredHashMap, DocId,
};

use super::RangeIndexBuildingSegmentData;

pub struct RangeIndexBuildingSegmentReader {
    base_docid: DocId,
    bottom_postings: LayeredHashMap<u64, BuildingPostingList>,
    higher_postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl RangeIndexBuildingSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<RangeIndexBuildingSegmentData>) -> Self {
        Self {
            base_docid,
            bottom_postings: index_data.bottom_postings.clone(),
            higher_postings: index_data.higher_postings.clone(),
        }
    }

    pub fn lookup(&self, bottom_keys: &[u64], higher_keys: &[u64]) -> Vec<SegmentPosting<'_>> {
        let mut segment_postings = vec![];
        // if let Some(posting) = self.bottom_postings.get(&keys[0]) {
        //     // segment_postings.push(SegmentPosting::new_building_segment(base_docid, building_posting_list))
        // }

        segment_postings
    }
}
