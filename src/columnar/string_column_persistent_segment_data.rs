use std::sync::Arc;

use arrow::array::{Array, StringArray};

use crate::DocId;

use super::ColumnSegmentData;

pub struct StringColumnPersistentSegmentData {
    pub values: Arc<StringArray>,
}

impl StringColumnPersistentSegmentData {
    pub fn new(values: Arc<StringArray>) -> Self {
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<&str> {
        Some(self.values.value(docid as usize))
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

impl ColumnSegmentData for StringColumnPersistentSegmentData {}
