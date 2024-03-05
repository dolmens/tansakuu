use arrow::array::{Array, StringArray};

use crate::DocId;

use super::ColumnPersistentSegmentData;

pub struct StringColumnPersistentSegmentReader {
    values: StringArray,
}

impl StringColumnPersistentSegmentReader {
    pub fn new(column_data: &ColumnPersistentSegmentData) -> Self {
        let values = column_data
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();

        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<&str> {
        if self.values.is_null(docid as usize) {
            None
        } else {
            Some(self.values.value(docid as usize))
        }
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}
