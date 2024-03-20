use std::sync::Arc;

use crate::{
    index::inverted_index::{
        InvertedIndexPersistentSegmentData, PersistentSegmentPosting, SegmentMultiPosting,
        SegmentMultiPostingData,
    },
    DocId,
};

pub struct SpatialIndexPersistentSegmentReader {
    base_docid: DocId,
    index_data: Arc<InvertedIndexPersistentSegmentData>,
}

impl SpatialIndexPersistentSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<InvertedIndexPersistentSegmentData>) -> Self {
        Self {
            base_docid,
            index_data,
        }
    }

    pub fn lookup(&self, hashkeys: &[u64]) -> Option<SegmentMultiPosting<'_>> {
        let postings: Vec<_> = hashkeys
            .iter()
            .filter_map(|&key| {
                self.index_data
                    .posting_data
                    .term_dict
                    .get(key.to_be_bytes())
                    .ok()
                    .unwrap()
            })
            .map(|term_info| PersistentSegmentPosting {
                term_info,
                posting_data: &self.index_data.posting_data,
            })
            .collect();
        if !postings.is_empty() {
            Some(SegmentMultiPosting::new(
                self.base_docid,
                SegmentMultiPostingData::Persistent(postings),
            ))
        } else {
            None
        }
    }
}
