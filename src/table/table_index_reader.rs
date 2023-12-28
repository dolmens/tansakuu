use crate::{
    index::{IndexReader, IndexReaderFactory, IndexReaderSnapshot, PostingIterator},
    query::Term,
};
use std::{collections::HashMap, ops::Deref};

use super::{TableData, TableDataSnapshot};

pub struct TableIndexReader {
    indexes: HashMap<String, Box<dyn IndexReader>>,
}

pub struct TableIndexReaderSnapshot<'a> {
    reader: &'a TableIndexReader,
    snapshot: &'a TableDataSnapshot,
}

impl TableIndexReader {
    pub fn new(table_data: &TableData) -> Self {
        let mut indexes = HashMap::new();
        let index_reader_factory = IndexReaderFactory::default();
        let schema = table_data.schema();
        for index in schema.indexes() {
            let index_reader = index_reader_factory.create(index, table_data);
            indexes.insert(index.name().to_string(), index_reader);
        }

        Self { indexes }
    }

    pub fn index(&self, name: &str) -> Option<&dyn IndexReader> {
        self.indexes.get(name).map(|r| r.deref())
    }

    pub fn lookup(
        &self,
        term: &Term,
        data_snapshot: &TableDataSnapshot,
    ) -> Option<Box<dyn PostingIterator>> {
        self.index(term.index_name())?
            .lookup(term.keyword(), data_snapshot)
    }
}

impl<'a> TableIndexReaderSnapshot<'a> {
    pub fn new(reader: &'a TableIndexReader, snapshot: &'a TableDataSnapshot) -> Self {
        Self { snapshot, reader }
    }
    pub fn index(&self, name: &str) -> Option<IndexReaderSnapshot> {
        self.reader
            .index(name)
            .map(|index| IndexReaderSnapshot::new(self.snapshot, index))
    }

    pub fn lookup(&self, term: &Term) -> Option<Box<dyn PostingIterator>> {
        self.index(term.index_name())?.lookup(term.keyword())
    }
}
