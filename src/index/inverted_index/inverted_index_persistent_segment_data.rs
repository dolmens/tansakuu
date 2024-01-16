use tantivy_common::OwnedBytes;

use crate::{index::IndexSegmentData, postings::TermDict};

pub struct InvertedIndexPersistentSegmentData {
    pub term_dict: TermDict,
    pub skip_data: OwnedBytes,
    pub posting_data: OwnedBytes,
}

impl InvertedIndexPersistentSegmentData {
    pub fn new(term_dict: TermDict, skip_data: OwnedBytes, posting_data: OwnedBytes) -> Self {
        Self {
            term_dict,
            skip_data,
            posting_data,
        }
    }
}

impl IndexSegmentData for InvertedIndexPersistentSegmentData {}
