use crate::{index::IndexSegmentData, DocId};

use super::PrimaryKeyDict;

pub struct PrimaryKeyPersistentSegmentData {
    pub keys: PrimaryKeyDict,
}

impl PrimaryKeyPersistentSegmentData {
    pub fn new(keys: PrimaryKeyDict) -> Self {
        Self { keys }
    }

    pub fn get_by_hashkey(&self, hashkey: u64) -> Option<DocId> {
        self.keys.get(hashkey.to_be_bytes()).ok().unwrap()
    }
}

impl IndexSegmentData for PrimaryKeyPersistentSegmentData {}
