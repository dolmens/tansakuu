use crate::schema::{Index, IndexType};

use super::{
    term::TermIndexSegmentDataBuilder, unique_key::UniqueKeyIndexSegmentDataBuilder,
    IndexSegmentDataBuilder,
};

#[derive(Default)]
pub struct IndexSegmentDataFactory {}

impl IndexSegmentDataFactory {
    pub fn create_builder(&self, index: &Index) -> Box<dyn IndexSegmentDataBuilder> {
        match index.index_type() {
            IndexType::Term => Box::new(TermIndexSegmentDataBuilder::new()),
            IndexType::UniqueKey => Box::new(UniqueKeyIndexSegmentDataBuilder::new()),
        }
    }
}
