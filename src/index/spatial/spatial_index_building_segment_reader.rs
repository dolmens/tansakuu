use std::sync::Arc;

use crate::{
    index::inverted_index::{
        BuildingSegmentPosting, InvertedIndexBuildingSegmentData, SegmentMultiPosting,
        SegmentMultiPostingData,
    },
    postings::BuildingPostingList,
    table::segment::BuildingDocCount,
    util::layered_hashmap::LayeredHashMap,
    DocId,
};

pub struct SpatialIndexBuildingSegmentReader {
    base_docid: DocId,
    doc_count: BuildingDocCount,
    postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl SpatialIndexBuildingSegmentReader {
    pub fn new(
        base_docid: DocId,
        doc_count: BuildingDocCount,
        index_data: Arc<InvertedIndexBuildingSegmentData>,
    ) -> Self {
        Self {
            base_docid,
            doc_count,
            postings: index_data.postings.clone(),
        }
    }

    pub fn lookup(&self, hashkeys: &[u64]) -> Option<SegmentMultiPosting<'_>> {
        let postings: Vec<_> = hashkeys
            .iter()
            .filter_map(|key| self.postings.get(key))
            .map(|building_posting_list| BuildingSegmentPosting {
                building_posting_list,
            })
            .collect();
        if !postings.is_empty() {
            Some(SegmentMultiPosting::new(
                self.base_docid,
                self.doc_count.get(),
                SegmentMultiPostingData::Building(postings),
            ))
        } else {
            None
        }
    }
}
