use std::{collections::hash_map::RandomState, sync::Arc};

use crate::{
    document::{OwnedValue, Value},
    index::IndexWriter,
    util::{
        capacity_policy::FixedCapacityPolicy, hash::hash_string_64,
        layered_hashmap::LayeredHashMapWriter,
    },
    DocId,
};

use super::PrimaryKeyBuildingSegmentData;

pub struct PrimaryKeyWriter {
    current_key: Option<String>,
    keys: LayeredHashMapWriter<u64, DocId>,
    index_data: Arc<PrimaryKeyBuildingSegmentData>,
}

impl PrimaryKeyWriter {
    pub fn new() -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let keys =
            LayeredHashMapWriter::with_initial_capacity(1024, hasher_builder, capacity_policy);
        let keymap = keys.hashmap();

        Self {
            current_key: None,
            keys,
            index_data: Arc::new(PrimaryKeyBuildingSegmentData::new(keymap)),
        }
    }
}

impl IndexWriter for PrimaryKeyWriter {
    fn add_field(&mut self, _field: &str, value: OwnedValue) {
        assert!(self.current_key.is_none());
        let token: String = value.try_into().unwrap();
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
