use std::sync::Arc;

use crate::{
    index::SegmentPosting,
    postings::{
        BuildingPostingList, BuildingPostingReader, DocListBlock, PostingFormat, PostingRead,
    },
    util::layered_hashmap::LayeredHashMap,
    DocId,
};

use super::InvertedIndexBuildingSegmentData;

pub struct InvertedIndexBuildingSegmentReader {
    base_docid: DocId,
    postings: LayeredHashMap<u64, BuildingPostingList>,
}

impl InvertedIndexBuildingSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<InvertedIndexBuildingSegmentData>) -> Self {
        Self {
            base_docid,
            postings: index_data.postings.clone(),
        }
    }

    pub fn segment_posting(&self, hashkey: u64) -> crate::index::SegmentPosting {
        let docids = if let Some(building_posting_list) = self.postings.get(&hashkey) {
            let mut docids = vec![];
            let mut posting_reader = BuildingPostingReader::open(building_posting_list);
            let posting_format = PostingFormat::default();
            let doc_list_format = posting_format.doc_list_format().clone();
            let mut doc_list_block = DocListBlock::new(&doc_list_format);
            loop {
                if !posting_reader
                    .decode_one_block(0, &mut doc_list_block)
                    .unwrap()
                {
                    break;
                }
                for &docid in &doc_list_block.docids[0..doc_list_block.len] {
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
