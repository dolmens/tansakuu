use std::sync::Arc;

use crate::{
    index::SegmentPosting,
    postings::{
        BuildingPostingList, BuildingPostingReader, PostingBlock, PostingFormat, PostingRead,
    },
    util::layered_hashmap::LayeredHashMap,
    DocId,
};

use super::InvertedIndexBuildingSegmentData;

pub struct InvertedIndexBuildingSegmentReader {
    base_docid: DocId,
    postings: LayeredHashMap<String, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<InvertedIndexBuildingSegmentData>) -> Self {
        Self {
            base_docid,
            postings: index_data.postings.clone(),
        }
    }

    pub fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let docids = if let Some(building_posting_list) = self.postings.get(tok) {
            let mut docids = vec![];
            let mut posting_reader = BuildingPostingReader::open(building_posting_list);
            let posting_format = PostingFormat::default();
            let mut posting_block = PostingBlock::new(&posting_format);
            loop {
                if !posting_reader
                    .decode_one_block(0, &mut posting_block)
                    .unwrap()
                {
                    break;
                }
                for &docid in &posting_block.docids[0..posting_block.len] {
                    docids.push(docid);
                }
            }
            docids
        } else {
            vec![]
        };
        SegmentPosting::new(self.base_docid, docids)
    }
}
