use std::sync::Mutex;

use crate::RowId;

use super::ColumnSegmentData;

pub struct GenericColumnSegmentData<T> {
    pub values: Mutex<Vec<T>>,
}

impl<T> GenericColumnSegmentData<T> {
    pub fn new() -> Self {
        Self {
            values: Mutex::new(Vec::new()),
        }
    }

    pub fn push(&self, value: T) {
        self.values.lock().unwrap().push(value);
    }

    pub fn get(&self, rowid: RowId) -> Option<T>
    where
        T: Clone,
    {
        self.values.lock().unwrap().get(rowid as usize).cloned()
    }

    pub fn doc_count(&self) -> usize {
        self.values.lock().unwrap().len()
    }
}

impl<T: Send + Sync + 'static> ColumnSegmentData for GenericColumnSegmentData<T> {}
