use std::{collections::HashMap, sync::Mutex};

use crate::{index::IndexSegmentData, DocId, END_DOCID};

pub struct UniqueKeyIndexBuildingSegmentData {
    keys: Mutex<HashMap<String, DocId>>,
}

impl UniqueKeyIndexBuildingSegmentData {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
        }
    }

    pub fn insert(&self, key: String, docid: DocId) {
        let mut keys = self.keys.lock().unwrap();
        keys.insert(key, docid);
    }

    pub fn lookup(&self, key: &str) -> DocId {
        let keys = self.keys.lock().unwrap();
        keys.get(key).cloned().unwrap_or(END_DOCID)
    }

    pub fn keys(&self) -> HashMap<String, DocId> {
        let keys = self.keys.lock().unwrap();
        keys.clone()
    }
}

impl IndexSegmentData for UniqueKeyIndexBuildingSegmentData {}
