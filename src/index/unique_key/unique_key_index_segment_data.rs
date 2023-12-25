use std::collections::HashMap;

use crate::{index::IndexSegmentData, DocId};

pub struct UniqueKeyIndexSegmentData {
    pub keys: HashMap<String, DocId>,
}

impl UniqueKeyIndexSegmentData {
    pub fn new(keys: HashMap<String, DocId>) -> Self {
        Self { keys }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.keys.get(key).cloned()
    }
}

impl IndexSegmentData for UniqueKeyIndexSegmentData {}
