use std::sync::Arc;

use crate::DocId;

use super::GenericColumnPersistentSegmentData;

pub struct GenericColumnPersistentSegmentReader<T> {
    column_data: Arc<GenericColumnPersistentSegmentData<T>>,
}

impl<T> GenericColumnPersistentSegmentReader<T> {
    pub fn new(column_data: Arc<GenericColumnPersistentSegmentData<T>>) -> Self {
        Self { column_data }
    }

    pub fn get(&self, docid: DocId) -> Option<T>
    where
        T: Clone,
    {
        self.column_data.get(docid)
    }

    pub fn doc_count(&self) -> usize {
        self.column_data.doc_count()
    }
}
