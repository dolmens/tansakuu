use crate::schema::{Index, IndexType};

use super::{inverted_index::InvertedIndexMerger, unique_key::UniqueKeyMerger, IndexMerger};

#[derive(Default)]
pub struct IndexMergerFactory {}

impl IndexMergerFactory {
    pub fn create(&self, index: &Index) -> Box<dyn IndexMerger> {
        match index.index_type() {
            IndexType::Text(_) => Box::new(InvertedIndexMerger::default()),
            IndexType::UniqueKey => Box::new(UniqueKeyMerger::default()),
        }
    }
}
