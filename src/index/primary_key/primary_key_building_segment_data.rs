use std::collections::{hash_map::RandomState, HashMap};

use crate::{
    index::IndexSegmentData,
    util::{FixedCapacityPolicy, LayeredHashMap},
    DocId,
};

pub struct PrimaryKeyBuildingSegmentData {
    keys: LayeredHashMap<String, DocId>,
}

impl PrimaryKeyBuildingSegmentData {
    pub fn new() -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let keys2 = LayeredHashMap::with_initial_capacity(1024, hasher_builder, capacity_policy);

        Self { keys: keys2 }
    }

    pub unsafe fn insert(&self, key: String, docid: DocId) {
        self.keys.insert(key, docid);
    }

    pub fn lookup(&self, key: &str) -> Option<DocId> {
        self.keys.get(key).cloned()
    }

    pub fn keys(&self) -> HashMap<String, DocId> {
        let mut keys = HashMap::new();
        for (k, v) in self.keys.iter() {
            keys.insert(k.clone(), *v);
        }
        keys
    }
}

impl IndexSegmentData for PrimaryKeyBuildingSegmentData {}
