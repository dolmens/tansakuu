use std::collections::HashMap;

use crate::index::{IndexReader, PostingIterator};

use super::TableData;

pub struct TableIndexReader {
    indexes: HashMap<String, Box<dyn IndexReader>>,
}

impl TableIndexReader {
    pub fn new(table_data: &TableData) -> Self {
        let schema = table_data.schema();
        for index in schema.indexes() {

        }

        Self {
            indexes: HashMap::new(),
        }
    }

    pub fn lookup() -> Option<Box<dyn PostingIterator>> {
        unimplemented!()
    }
}
