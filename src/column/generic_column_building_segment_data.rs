use std::sync::Mutex;

use crate::DocId;

use super::ColumnSegmentData;

pub struct GenericColumnBuildingSegmentData<T> {
    pub values: Mutex<Vec<T>>,
}

impl<T> GenericColumnBuildingSegmentData<T> {
    pub fn new() -> Self {
        Self {
            values: Mutex::new(Vec::new()),
        }
    }

    pub fn push(&self, value: T) {
        self.values.lock().unwrap().push(value);
    }

    pub fn get(&self, docid: DocId) -> Option<T>
    where
        T: Clone,
    {
        self.values.lock().unwrap().get(docid as usize).cloned()
    }

    pub fn values(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.values.lock().unwrap().clone()
    }

    pub fn doc_count(&self) -> usize {
        self.values.lock().unwrap().len()
    }
}

impl<T: Send + Sync + 'static> ColumnSegmentData for GenericColumnBuildingSegmentData<T> {}
