use std::sync::Arc;

use crate::DocId;

use super::{
    persistent_posting_reader::PersistentPostingReader,
    InvertedIndexPersistentSegmentData, SegmentPosting,
};

pub struct InvertedIndexPersistentSegmentReader {
    base_docid: DocId,
    index_data: Arc<InvertedIndexPersistentSegmentData>,
}

impl InvertedIndexPersistentSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<InvertedIndexPersistentSegmentData>) -> Self {
        Self {
            base_docid,
            index_data,
        }
    }

    pub fn segment_posting(&self, hashkey: u64) -> Option<SegmentPosting<'_>> {
        if let Some(term_info) = self
            .index_data
            .posting_data
            .term_dict
            .get(hashkey.to_be_bytes())
            .ok()
            .unwrap()
        {
            Some(SegmentPosting::new_persistent_segment(
                self.base_docid,
                term_info,
                &self.index_data.posting_data,
            ))
        } else {
            None
        }
    }

    pub fn posting_reader(&self, hashkey: u64) -> Option<PersistentPostingReader<'_>> {
        PersistentPostingReader::lookup(&self.index_data.posting_data, hashkey).unwrap()
    }
}
