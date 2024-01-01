use std::collections::HashMap;

use crate::{index::IndexSegmentData, DocId};

pub struct TermIndexPersistentSegmentData {
    pub postings: HashMap<String, Vec<DocId>>,
}

impl TermIndexPersistentSegmentData {
    pub fn new(postings: HashMap<String, Vec<DocId>>) -> Self {
        Self {
            postings,
        }
    }
}

impl IndexSegmentData for TermIndexPersistentSegmentData {}
