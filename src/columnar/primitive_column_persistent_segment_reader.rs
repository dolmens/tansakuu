use arrow::array::{Array, PrimitiveArray};

use crate::{types::PrimitiveType, DocId};

use super::ColumnPersistentSegmentData;

pub struct PrimitiveColumnPersistentSegmentReader<T: PrimitiveType> {
    values: PrimitiveArray<T::ArrowPrimitive>,
}

impl<T: PrimitiveType> PrimitiveColumnPersistentSegmentReader<T> {
    pub fn new(column_data: &ColumnPersistentSegmentData) -> Self {
        let values = column_data
            .array()
            .as_any()
            .downcast_ref::<PrimitiveArray<T::ArrowPrimitive>>()
            .unwrap()
            .clone();

        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<T::Native> {
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
