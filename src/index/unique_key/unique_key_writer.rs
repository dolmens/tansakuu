use std::{collections::hash_map::RandomState, sync::Arc};

use crate::{
    document::{OwnedValue, Value},
    index::{inverted_index::TokenHasher, IndexWriter, IndexWriterResource},
    schema::{FieldRef, FieldType},
    util::{capacity_policy::FixedCapacityPolicy, layered_hashmap::LayeredHashMapWriter},
    DocId, HASHMAP_INITIAL_CAPACITY,
};

use super::UniqueKeyBuildingSegmentData;

pub struct UniqueKeyWriter {
    current_key: u64,
    keys: LayeredHashMapWriter<u64, DocId>,
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

        Self {
            current_key: 0,
            keys,
        }
    }
}

impl IndexWriter for UniqueKeyWriter {
    fn add_field(&mut self, field: &FieldRef, value: &OwnedValue) {
        let token_hasher = TokenHasher::default();
        self.current_key = match field.data_type() {
            FieldType::Int8 | FieldType::Int16 | FieldType::Int32 | FieldType::Int64 => {
                match value.as_i64() {
                    Some(value) => token_hasher.hash_bytes(value.to_string().as_bytes()),
                    None => {
                        return;
                    }
                }
            }
            FieldType::UInt8 | FieldType::UInt16 | FieldType::UInt32 | FieldType::UInt64 => {
                match value.as_u64() {
                    Some(value) => token_hasher.hash_bytes(value.to_string().as_bytes()),
                    None => {
                        return;
                    }
                }
            }
            FieldType::Str | FieldType::Text => match value.as_str() {
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
        Arc::new(UniqueKeyBuildingSegmentData::new(self.keys.hashmap()))
    }
}
