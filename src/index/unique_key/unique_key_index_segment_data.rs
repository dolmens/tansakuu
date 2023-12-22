use std::collections::HashMap;

use crate::{index::IndexSegmentData, DocId, END_DOCID};

pub struct UniqueKeyIndexSegmentData {
    keys: HashMap<String, DocId>,
}

impl UniqueKeyIndexSegmentData {
    pub fn new(keys: HashMap<String, DocId>) -> Self {
        Self { keys }
    }

    pub fn lookup(&self, key: &str) -> DocId {
        self.keys.get(key).cloned().unwrap_or(END_DOCID)
    }
}

impl IndexSegmentData for UniqueKeyIndexSegmentData {}
