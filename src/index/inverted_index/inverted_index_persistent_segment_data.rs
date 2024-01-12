use std::collections::HashMap;

use tantivy_common::OwnedBytes;

use crate::{index::IndexSegmentData, DocId};

pub struct InvertedIndexPersistentSegmentData {
    pub postings: HashMap<String, Vec<DocId>>,
    pub bytes: OwnedBytes,
}

impl InvertedIndexPersistentSegmentData {
    pub fn new(postings: HashMap<String, Vec<DocId>>, bytes: OwnedBytes) -> Self {
        Self { postings, bytes }
    }
}

impl IndexSegmentData for InvertedIndexPersistentSegmentData {}
