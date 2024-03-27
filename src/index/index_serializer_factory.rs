use crate::schema::{IndexRef, IndexType};

use super::{
    bitset::BitsetIndexSerializer, inverted_index::InvertedIndexSerializer,
    range::RangeIndexSerializer, unique_key::UniqueKeySerializer, IndexSerializer,
};

#[derive(Default)]
pub struct IndexSerializerFactory {}

impl IndexSerializerFactory {
    pub fn create(&self, index: &IndexRef) -> Box<dyn IndexSerializer> {
        match index.index_type() {
            IndexType::Text(_) => Box::new(InvertedIndexSerializer::default()),
            IndexType::PrimaryKey => Box::new(UniqueKeySerializer::default()),
            IndexType::UniqueKey => Box::new(UniqueKeySerializer::default()),
            IndexType::Bitset => Box::new(BitsetIndexSerializer::default()),
            IndexType::Range => Box::new(RangeIndexSerializer::default()),
            IndexType::Spatial(_) => Box::new(InvertedIndexSerializer::default()),
        }
    }
}
