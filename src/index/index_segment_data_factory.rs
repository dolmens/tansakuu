use crate::schema::{Index, IndexType};

use super::{
    inverted_index::InvertedIndexSegmentDataBuilder,
    primary_key::PrimaryKeySegmentDataBuilder, IndexSegmentDataBuilder,
};

#[derive(Default)]
pub struct IndexSegmentDataFactory {}

impl IndexSegmentDataFactory {
    pub fn create_builder(&self, index: &Index) -> Box<dyn IndexSegmentDataBuilder> {
        match index.index_type() {
            IndexType::InvertedIndex => Box::new(InvertedIndexSegmentDataBuilder::new()),
            IndexType::PrimaryKey => Box::new(PrimaryKeySegmentDataBuilder::new()),
        }
    }
}
