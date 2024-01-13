use std::collections::HashMap;

use tantivy_common::OwnedBytes;

use crate::{index::IndexSegmentData, postings::TermDict, DocId};

pub struct InvertedIndexPersistentSegmentData {
    pub postings: HashMap<String, Vec<DocId>>,
    pub term_dict: TermDict,
    pub posting_data: OwnedBytes,
}

impl InvertedIndexPersistentSegmentData {
    pub fn new(
        postings: HashMap<String, Vec<DocId>>,
        term_dict: TermDict,
        posting_data: OwnedBytes,
    ) -> Self {
        Self {
            postings,
            term_dict,
            posting_data,
        }
    }
}

impl IndexSegmentData for InvertedIndexPersistentSegmentData {}
