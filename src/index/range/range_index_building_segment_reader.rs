use std::sync::Arc;

use crate::{
    index::inverted_index::{BuildingSegmentPosting, SegmentMultiPosting, SegmentMultiPostingData},
    postings::BuildingPostingList,
    util::layered_hashmap::LayeredHashMap,
    DocId,
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

    pub fn lookup(
        &self,
        bottom_keys: &[u64],
        higher_keys: &[u64],
    ) -> Option<SegmentMultiPosting<'_>> {
        let postings: Vec<_> = bottom_keys
            .iter()
            .filter_map(|&k| self.bottom_postings.get(&k))
            .chain(
                higher_keys
                    .iter()
                    .filter_map(|&k| self.higher_postings.get(&k)),
            )
            .map(|building_posting_list| BuildingSegmentPosting {
                building_posting_list,
            })
            .collect();

        if !postings.is_empty() {
            Some(SegmentMultiPosting::new(
                self.base_docid,
                SegmentMultiPostingData::Building(postings),
            ))
        } else {
            None
        }
    }
}
