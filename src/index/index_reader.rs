use crate::table::TableDataSnapshot;

use super::PostingIterator;

pub trait IndexReader: Send + Sync {
    fn lookup(
        &self,
        key: &str,
        data_snapshot: &TableDataSnapshot,
    ) -> Option<Box<dyn PostingIterator>>;
}

pub struct IndexReaderSnapshot<'a> {
    data_snapshot: &'a TableDataSnapshot,
    index_reader: &'a dyn IndexReader,
}

impl<'a> IndexReaderSnapshot<'a> {
    pub fn new(data_snapshot: &'a TableDataSnapshot, index_reader: &'a dyn IndexReader) -> Self {
        Self {
            data_snapshot,
            index_reader,
        }
    }

    pub fn lookup(&self, key: &str) -> Option<Box<dyn PostingIterator>> {
        self.index_reader.lookup(key, self.data_snapshot)
    }
}
