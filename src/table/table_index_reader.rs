use std::{collections::HashMap, ops::Deref};

use crate::{
    index::{IndexReader, IndexReaderFactory, PostingIterator},
    query::Term,
};

use super::TableData;

pub struct TableIndexReader {
    indexes: HashMap<String, Box<dyn IndexReader>>,
}

impl TableIndexReader {
    pub fn new(table_data: &TableData) -> Self {
        let mut indexes = HashMap::new();
        let index_reader_factory = IndexReaderFactory::new();
        let schema = table_data.schema();
        for index in schema.indexes() {
            let index_reader = index_reader_factory.create(index, table_data);
            indexes.insert(index.name().to_string(), index_reader);
        }

        Self { indexes }
    }

    pub fn index_reader(&self, name: &str) -> Option<&dyn IndexReader> {
        self.indexes.get(name).map(|r| r.deref())
    }

    pub fn lookup(&self, term: &Term) -> Option<Box<dyn PostingIterator>> {
        self.indexes.get(term.index_name())?.lookup(term.keyword())
    }
}
