use std::collections::HashMap;

use crate::{index::IndexSegmentData, DocId};

pub struct PrimaryKeyPersistentSegmentData {
    pub keys: HashMap<String, DocId>,
}

impl PrimaryKeyPersistentSegmentData {
    pub fn new(keys: HashMap<String, DocId>) -> Self {
        Self { keys }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.keys.get(key).cloned()
    }
}

impl IndexSegmentData for PrimaryKeyPersistentSegmentData {}
