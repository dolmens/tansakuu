use crate::{
    index::{IndexReader, IndexReaderFactory, PostingIterator},
    query::Term,
};
use std::{collections::HashMap, ops::Deref, sync::Arc};

use super::TableData;

pub struct TableIndexReader {
    indexes: HashMap<String, Arc<dyn IndexReader>>,
}

impl TableIndexReader {
    pub fn new(table_data: &TableData) -> Self {
        let mut indexes = HashMap::new();
        let index_reader_factory = IndexReaderFactory::default();
        let schema = table_data.schema();
        for index in schema.indexes() {
            let index_reader = index_reader_factory.create(index, table_data);
            indexes.insert(index.name().to_string(), index_reader.into());
        }

        Self { indexes }
    }

    pub fn index(&self, name: &str) -> Option<&dyn IndexReader> {
        self.indexes.get(name).map(|r| r.deref())
    }

    pub fn lookup(&self, term: &Term) -> Option<Box<dyn PostingIterator>> {
        self.index(term.index_name())?.lookup(term.keyword())
    }

    pub(crate) fn index_ref(&self, name: &str) -> Option<Arc<dyn IndexReader>> {
        self.indexes.get(name).cloned()
    }
}
