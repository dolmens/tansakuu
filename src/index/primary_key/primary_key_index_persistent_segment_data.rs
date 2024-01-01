use std::collections::HashMap;

use crate::{index::IndexSegmentData, DocId};

pub struct PrimaryKeyIndexPersistentSegmentData {
    pub keys: HashMap<String, DocId>,
}

impl PrimaryKeyIndexPersistentSegmentData {
    pub fn new(keys: HashMap<String, DocId>) -> Self {
        Self { keys }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.keys.get(key).cloned()
    }
}

impl IndexSegmentData for PrimaryKeyIndexPersistentSegmentData {}
