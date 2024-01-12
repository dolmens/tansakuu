use crate::schema::{Index, IndexType};

use super::{inverted_index::InvertedIndexMerger, primary_key::PrimaryKeyMerger, IndexMerger};

#[derive(Default)]
pub struct IndexMergerFactory {}

impl IndexMergerFactory {
    pub fn create(&self, index: &Index) -> Box<dyn IndexMerger> {
        match index.index_type() {
            IndexType::InvertedIndex => Box::new(InvertedIndexMerger::default()),
            IndexType::PrimaryKey => Box::new(PrimaryKeyMerger::default()),
        }
    }
}
