use crate::DocId;

use super::ColumnSegmentData;

pub struct GenericColumnSegmentData<T> {
    pub values: Vec<T>,
}

impl<T> GenericColumnSegmentData<T> {
    pub fn new(values: Vec<T>) -> Self {
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<T>
    where
        T: Clone,
    {
        self.values.get(docid as usize).cloned()
    }

    pub fn values(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.values.clone()
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

impl<T: Send + Sync + 'static> ColumnSegmentData for GenericColumnSegmentData<T> {}
