use crate::schema::{Index, IndexType};

use super::{
    inverted_index::InvertedIndexSegmentDataBuilder, unique_key::UniqueKeySegmentDataBuilder,
    IndexSegmentDataBuilder,
};

#[derive(Default)]
pub struct IndexSegmentDataFactory {}

impl IndexSegmentDataFactory {
    pub fn create_builder(&self, index: &Index) -> Box<dyn IndexSegmentDataBuilder> {
        match index.index_type() {
            IndexType::Text(_) => Box::new(InvertedIndexSegmentDataBuilder::new()),
            IndexType::UniqueKey => Box::new(UniqueKeySegmentDataBuilder::new()),
        }
    }
}
