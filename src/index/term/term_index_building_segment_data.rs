use std::{collections::HashMap, sync::Mutex};

use crate::{index::IndexSegmentData, DocId};

pub struct TermIndexBuildingSegmentData {
    pub postings: Mutex<HashMap<String, Vec<DocId>>>,
}

impl TermIndexBuildingSegmentData {
    pub fn new() -> Self {
        Self {
            postings: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_doc(&self, tok: String, docid: DocId) {
        let mut postings = self.postings.lock().unwrap();
        postings.entry(tok).or_insert_with(Vec::new).push(docid);
    }

    pub fn postings(&self) -> HashMap<String, Vec<DocId>> {
        let mut postings = self.postings.lock().unwrap();
        postings.clone()
    }
}

impl IndexSegmentData for TermIndexBuildingSegmentData {}
