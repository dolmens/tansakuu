use arrow::array::{Array, BooleanArray};

use crate::DocId;

use super::ColumnPersistentSegmentData;

pub struct BooleanColumnPersistentSegmentReader {
    values: BooleanArray,
}

impl BooleanColumnPersistentSegmentReader {
    pub fn new(column_data: &ColumnPersistentSegmentData) -> Self {
        let values = column_data
            .array()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();

        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<bool> {
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
