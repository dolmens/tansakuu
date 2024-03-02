use std::{collections::hash_map::RandomState, sync::Arc};

use crate::{
    document::{OwnedValue, Value},
    index::IndexWriter,
    table::SegmentStat,
    util::{
        capacity_policy::FixedCapacityPolicy, hash::hash_string_64,
        layered_hashmap::LayeredHashMapWriter,
    },
    DocId, HASHMAP_INITIAL_CAPACITY,
};

use super::UniqueKeyBuildingSegmentData;

pub struct UniqueKeyWriter {
    current_key: Option<String>,
    keys: LayeredHashMapWriter<u64, DocId>,
    index_data: Arc<UniqueKeyBuildingSegmentData>,
}

impl UniqueKeyWriter {
    pub fn new(recent_segment_stat: Option<&Arc<SegmentStat>>) -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let hashmap_initial_capacity = recent_segment_stat
            .map(|stat| stat.doc_count)
            .unwrap_or(HASHMAP_INITIAL_CAPACITY);
        let hashmap_initial_capacity = if hashmap_initial_capacity > 0 {
            hashmap_initial_capacity
        } else {
            HASHMAP_INITIAL_CAPACITY
        };
        let keys = LayeredHashMapWriter::with_capacity(
            hashmap_initial_capacity,
            hasher_builder,
            capacity_policy,
        );
        let keymap = keys.hashmap();

        Self {
            current_key: None,
            keys,
            index_data: Arc::new(UniqueKeyBuildingSegmentData::new(keymap)),
        }
    }
}

impl IndexWriter for UniqueKeyWriter {
    fn add_field(&mut self, _field: &str, value: &OwnedValue) {
        assert!(self.current_key.is_none());
        let token: String = value.as_i64().unwrap_or_default().to_string();
        self.current_key = Some(token);
    }

    fn end_document(&mut self, docid: DocId) {
        assert!(self.current_key.is_some());
        let hashkey = hash_string_64(self.current_key.take().unwrap().as_str());
        self.keys.insert(hashkey, docid);
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
