use crate::{index::IndexSegmentData, DocId};

use super::UniqueKeyDict;

pub struct UniqueKeyPersistentSegmentData {
    pub keys: UniqueKeyDict,
}

impl UniqueKeyPersistentSegmentData {
    pub fn new(keys: UniqueKeyDict) -> Self {
        Self { keys }
    }

    pub fn get_by_hashkey(&self, hashkey: u64) -> Option<DocId> {
        self.keys.get(hashkey.to_be_bytes()).ok().unwrap()
    }
}

impl IndexSegmentData for UniqueKeyPersistentSegmentData {}
