use crate::schema::{Index, IndexType};

use super::{primary_key::PrimaryKeyIndexMerger, term::TermIndexMerger, IndexMerger};

#[derive(Default)]
pub struct IndexMergerFactory {}

impl IndexMergerFactory {
    pub fn create(&self, index: &Index) -> Box<dyn IndexMerger> {
        match index.index_type() {
            IndexType::Term => Box::new(TermIndexMerger::default()),
            IndexType::PrimaryKey => Box::new(PrimaryKeyIndexMerger::default()),
        }
    }
}
