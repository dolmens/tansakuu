use std::collections::HashMap;

use crate::{index::IndexSegmentData, DocId};

pub struct TermIndexSegmentData {
    pub postings: HashMap<String, Vec<DocId>>,
}

impl TermIndexSegmentData {
    pub fn new(postings: HashMap<String, Vec<DocId>>) -> Self {
        Self {
            postings,
        }
    }
}

impl IndexSegmentData for TermIndexSegmentData {}
