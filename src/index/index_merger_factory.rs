use crate::schema::{Index, IndexType};

use super::{term::TermIndexMerger, unique_key::UniqueKeyIndexMerger, IndexMerger};

#[derive(Default)]
pub struct IndexMergerFactory {}

impl IndexMergerFactory {
    pub fn create(&self, index: &Index) -> Box<dyn IndexMerger> {
        match index.index_type() {
            IndexType::Term => Box::new(TermIndexMerger::default()),
            IndexType::UniqueKey => Box::new(UniqueKeyIndexMerger::default()),
        }
    }
}
