use crate::{
    index::IndexSegmentData,
    postings::{BuildingPostingList, PostingFormat},
    schema::IndexRef,
    util::layered_hashmap::LayeredHashMap,
};

pub struct InvertedIndexBuildingSegmentData {
    pub index: IndexRef,
    pub posting_format: PostingFormat,
    pub postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentData {
    pub fn new(
        index: IndexRef,
        posting_format: PostingFormat,
        postings: LayeredHashMap<u64, BuildingPostingList>,
    ) -> Self {
        Self {
            index,
            posting_format,
            postings,
        }
    }
}

impl IndexSegmentData for InvertedIndexBuildingSegmentData {
    fn collect_stat(&self, segment_stat: &mut crate::table::SegmentStat) {
        segment_stat
            .index_term_count
            .insert(self.index.name().to_string(), self.postings.len());
    }
}
