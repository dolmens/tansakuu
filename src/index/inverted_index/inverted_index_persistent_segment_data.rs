use tantivy_common::OwnedBytes;

use crate::{
    index::IndexSegmentData,
    postings::{PostingFormat, TermDict},
};

pub struct InvertedIndexPersistentSegmentData {
    pub posting_format: PostingFormat,
    pub term_dict: TermDict,
    pub skip_list_data: OwnedBytes,
    pub doc_list_data: OwnedBytes,
    pub position_skip_list_data: OwnedBytes,
    pub position_list_data: OwnedBytes,
}

impl InvertedIndexPersistentSegmentData {
    pub fn new(
        posting_format: PostingFormat,
        term_dict: TermDict,
        skip_list_data: OwnedBytes,
        doc_list_data: OwnedBytes,
        position_skip_list_data: OwnedBytes,
        position_list_data: OwnedBytes,
    ) -> Self {
        Self {
            posting_format,
            term_dict,
            skip_list_data,
            doc_list_data,
            position_skip_list_data,
            position_list_data,
        }
    }
}

impl IndexSegmentData for InvertedIndexPersistentSegmentData {}
