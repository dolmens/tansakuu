use std::collections::HashMap;

use tantivy_common::OwnedBytes;

use crate::{index::IndexSegmentData, DocId};

pub struct TermIndexPersistentSegmentData {
    pub postings: HashMap<String, Vec<DocId>>,
    pub bytes: OwnedBytes,
}

impl TermIndexPersistentSegmentData {
    pub fn new(postings: HashMap<String, Vec<DocId>>, bytes: OwnedBytes) -> Self {
        Self { postings, bytes }
    }
}

impl IndexSegmentData for TermIndexPersistentSegmentData {}
