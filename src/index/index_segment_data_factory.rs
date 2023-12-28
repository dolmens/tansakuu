use crate::schema::{Index, IndexType};

use super::{
    primary_key::PrimaryKeyIndexSegmentDataBuilder, term::TermIndexSegmentDataBuilder,
    IndexSegmentDataBuilder,
};

#[derive(Default)]
pub struct IndexSegmentDataFactory {}

impl IndexSegmentDataFactory {
    pub fn create_builder(&self, index: &Index) -> Box<dyn IndexSegmentDataBuilder> {
        match index.index_type() {
            IndexType::Term => Box::new(TermIndexSegmentDataBuilder::new()),
            IndexType::PrimaryKey => Box::new(PrimaryKeyIndexSegmentDataBuilder::new()),
        }
    }
}
