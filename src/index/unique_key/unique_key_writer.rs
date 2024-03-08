use std::{collections::hash_map::RandomState, sync::Arc};

use crate::{
    document::{OwnedValue, Value},
    index::{inverted_index::TokenHasher, IndexWriter, IndexWriterResource},
    schema::{DataType, FieldRef},
    util::{capacity_policy::FixedCapacityPolicy, layered_hashmap::LayeredHashMapWriter},
    DocId, HASHMAP_INITIAL_CAPACITY,
};

use super::UniqueKeyBuildingSegmentData;

pub struct UniqueKeyWriter {
    current_key: u64,
    keys: LayeredHashMapWriter<u64, DocId>,
    index_data: Arc<UniqueKeyBuildingSegmentData>,
}

impl UniqueKeyWriter {
    pub fn new(writer_resource: &IndexWriterResource) -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let hashmap_initial_capacity = writer_resource
            .recent_segment_stat()
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
            current_key: 0,
            keys,
            index_data: Arc::new(UniqueKeyBuildingSegmentData::new(keymap)),
        }
    }
}

impl IndexWriter for UniqueKeyWriter {
    fn add_field(&mut self, field: &FieldRef, value: &OwnedValue) {
        let token_hasher = TokenHasher::default();
        self.current_key = match field.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                match value.as_i64() {
                    Some(value) => token_hasher.hash_bytes(value.to_string().as_bytes()),
                    None => {
                        return;
                    }
                }
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                match value.as_u64() {
                    Some(value) => token_hasher.hash_bytes(value.to_string().as_bytes()),
                    None => {
                        return;
                    }
                }
            }
            DataType::Str | DataType::Text => match value.as_str() {
                Some(value) => token_hasher.hash_bytes(value.as_bytes()),
                None => {
                    return;
                }
            },
            _ => {
                return;
            }
        };
    }

    fn end_document(&mut self, docid: DocId) {
        if self.current_key != 0 {
            self.keys.insert(self.current_key, docid);
            self.current_key = 0;
        }
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
