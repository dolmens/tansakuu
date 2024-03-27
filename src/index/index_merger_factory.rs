use crate::schema::{Index, IndexType};

use super::{
    bitset::BitsetIndexMerger, inverted_index::InvertedIndexMerger, range::RangeIndexMerger,
    unique_key::UniqueKeyMerger, IndexMerger,
};

#[derive(Default)]
pub struct IndexMergerFactory {}

impl IndexMergerFactory {
    pub fn create(&self, index: &Index) -> Box<dyn IndexMerger> {
        match index.index_type() {
            IndexType::Text(_) => Box::new(InvertedIndexMerger::default()),
            IndexType::PrimaryKey => Box::new(UniqueKeyMerger::default()),
            IndexType::UniqueKey => Box::new(UniqueKeyMerger::default()),
            IndexType::Bitset => Box::new(BitsetIndexMerger::default()),
            IndexType::Range => Box::new(RangeIndexMerger::default()),
            IndexType::Spatial(_) => Box::new(InvertedIndexMerger::default()),
        }
    }
}
