use std::sync::Arc;

use arrow::array::PrimitiveArray;

use crate::{types::PrimitiveType, DocId};

use super::ColumnSegmentData;

pub struct PrimitiveColumnPersistentSegmentData<T: PrimitiveType> {
    pub values: Arc<PrimitiveArray<T::ArrowPrimitive>>,
}

impl<T: PrimitiveType> PrimitiveColumnPersistentSegmentData<T> {
    pub fn new(values: Arc<PrimitiveArray<T::ArrowPrimitive>>) -> Self {
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<T::Native> {
        Some(self.values.value(docid as usize))
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

impl<T: PrimitiveType> ColumnSegmentData for PrimitiveColumnPersistentSegmentData<T> {}
