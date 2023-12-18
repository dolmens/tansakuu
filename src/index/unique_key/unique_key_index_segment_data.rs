use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{index::IndexSegmentData, DocId, END_DOCID};

pub struct UniqueKeyIndexSegmentData {
    keys: Mutex<HashMap<String, DocId>>,
}

pub struct UniqueKeyIndexSegmentWriter {}

pub struct UniqueKeyIndexSegmentReader {
    index_data: Arc<UniqueKeyIndexSegmentData>,
}

impl IndexSegmentData for UniqueKeyIndexSegmentData {}

impl UniqueKeyIndexSegmentReader {
    pub fn new(index_data: Arc<UniqueKeyIndexSegmentData>) -> Self {
        Self { index_data }
    }

    pub fn lookup(&self, key: &str) -> DocId {
        let keys = self.index_data.keys.lock().unwrap();
        keys.get(key).cloned().unwrap_or(END_DOCID)
    }
}
