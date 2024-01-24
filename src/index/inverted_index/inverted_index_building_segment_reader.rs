use std::sync::Arc;

use crate::{
    index::SegmentPosting,
    postings::{BuildingPostingReader, PostingBlock, PostingFormat, PostingRead},
    DocId,
};

use super::InvertedIndexBuildingSegmentData;

pub struct InvertedIndexBuildingSegmentReader {
    base_docid: DocId,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
}

impl InvertedIndexBuildingSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<InvertedIndexBuildingSegmentData>) -> Self {
        Self {
            base_docid,
            index_data,
        }
    }

    pub fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let docids = if let Some(building_posting_list) = self.index_data.postings.get(tok) {
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
