use std::collections::HashMap;

use crate::{index::IndexSegmentData, DocId};

pub struct PrimaryKeyIndexSegmentData {
    pub keys: HashMap<String, DocId>,
}

impl PrimaryKeyIndexSegmentData {
    pub fn new(keys: HashMap<String, DocId>) -> Self {
        Self { keys }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.keys.get(key).cloned()
    }
}

impl IndexSegmentData for PrimaryKeyIndexSegmentData {}
