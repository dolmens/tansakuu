use tantivy_common::OwnedBytes;

use crate::{index::IndexSegmentData, postings::TermDict};

pub struct InvertedIndexPersistentSegmentData {
    pub term_dict: TermDict,
    pub skip_list_data: OwnedBytes,
    pub posting_data: OwnedBytes,
    pub position_skip_list_data: OwnedBytes,
    pub position_list_data: OwnedBytes,
}

impl InvertedIndexPersistentSegmentData {
    pub fn new(
        term_dict: TermDict,
        skip_list_data: OwnedBytes,
        posting_data: OwnedBytes,
        position_skip_list_data: OwnedBytes,
        position_list_data: OwnedBytes,
    ) -> Self {
        Self {
            term_dict,
            skip_list_data,
            posting_data,
            position_skip_list_data,
            position_list_data,
        }
    }
}

impl IndexSegmentData for InvertedIndexPersistentSegmentData {}
