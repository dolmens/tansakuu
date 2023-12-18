use crate::{
    schema::{Index, IndexType},
    table::TableData,
};

use super::{IndexReader, TermIndexReader, UniqueKeyIndexReader};

pub struct IndexReaderFactory {}

impl IndexReaderFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create(&self, index: &Index, table_data: &TableData) -> Box<dyn IndexReader> {
        match index.index_type() {
            IndexType::Term => Box::new(TermIndexReader::new(index, table_data)),
            IndexType::UniqueKey => Box::new(UniqueKeyIndexReader::new(index, table_data)),
        }
    }
}
