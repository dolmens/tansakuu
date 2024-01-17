use crate::{index::IndexSegmentData, DocId};

use super::PrimaryKeyDict;

pub struct PrimaryKeyPersistentSegmentData {
    pub keys: PrimaryKeyDict,
}

impl PrimaryKeyPersistentSegmentData {
    pub fn new(keys: PrimaryKeyDict) -> Self {
        Self { keys }
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.keys.get(key).ok().unwrap()
    }
}

impl IndexSegmentData for PrimaryKeyPersistentSegmentData {}
