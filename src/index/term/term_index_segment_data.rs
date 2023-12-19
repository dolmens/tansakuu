use std::{collections::HashMap, sync::Mutex};

use crate::{index::IndexSegmentData, DocId};

pub struct TermIndexSegmentData {
    pub postings: Mutex<HashMap<String, Vec<DocId>>>,
}

impl TermIndexSegmentData {
    pub fn new() -> Self {
        Self {
            postings: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_doc(&self, tok: String, docid: DocId) {
        let mut postings = self.postings.lock().unwrap();
        postings.entry(tok).or_insert_with(Vec::new).push(docid);
    }
}

impl IndexSegmentData for TermIndexSegmentData {}
