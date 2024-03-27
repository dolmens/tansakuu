use std::sync::Arc;

use crate::{
    postings::BuildingPostingList, table::segment::BuildingDocCount,
    util::layered_hashmap::LayeredHashMap, DocId,
};

use super::{InvertedIndexBuildingSegmentData, SegmentPosting};

pub struct InvertedIndexBuildingSegmentReader {
    base_docid: DocId,
    doc_count: BuildingDocCount,
    postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentReader {
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

    pub fn segment_posting(&self, hashkey: u64) -> Option<SegmentPosting<'_>> {
        if let Some(building_posting_list) = self.postings.get(&hashkey) {
            Some(SegmentPosting::new_building_segment(
                self.base_docid,
                self.doc_count.get(),
                building_posting_list,
            ))
        } else {
            None
        }
    }
}
