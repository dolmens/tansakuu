use crate::{
    schema::{Index, IndexType},
    table::TableData,
};

use super::{IndexReader, InvertedIndexReader, UniqueKeyReader};

#[derive(Default)]
pub struct IndexReaderFactory {}

impl IndexReaderFactory {
    pub fn create(&self, index: &Index, table_data: &TableData) -> Box<dyn IndexReader> {
        match index.index_type() {
            IndexType::Text(_) => Box::new(InvertedIndexReader::new(index, table_data)),
            IndexType::UniqueKey => Box::new(UniqueKeyReader::new(index, table_data)),
        }
    }
}
