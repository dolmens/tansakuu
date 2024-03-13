use crate::schema::{Index, IndexType};

use super::{
    inverted_index::InvertedIndexSegmentDataBuilder, range::RangeIndexSegmentDataBuilder,
    unique_key::UniqueKeySegmentDataBuilder, IndexSegmentDataBuilder,
};

#[derive(Default)]
pub struct IndexSegmentDataFactory {}

impl IndexSegmentDataFactory {
    pub fn create_builder(&self, index: &Index) -> Box<dyn IndexSegmentDataBuilder> {
        match index.index_type() {
            IndexType::Text(_) => Box::new(InvertedIndexSegmentDataBuilder::default()),
            IndexType::PrimaryKey => Box::new(UniqueKeySegmentDataBuilder::default()),
            IndexType::UniqueKey => Box::new(UniqueKeySegmentDataBuilder::default()),
            IndexType::Range => Box::new(RangeIndexSegmentDataBuilder::default()),
        }
    }
}
