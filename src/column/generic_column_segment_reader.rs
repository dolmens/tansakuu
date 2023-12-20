use std::sync::Arc;

use crate::RowId;

use super::{ColumnSegmentReader, GenericColumnSegmentData};

pub struct GenericColumnSegmentReader<T> {
    column_data: Arc<GenericColumnSegmentData<T>>,
}

impl<T> GenericColumnSegmentReader<T> {
    pub fn new(column_data: Arc<GenericColumnSegmentData<T>>) -> Self {
        Self { column_data }
    }

    pub fn get(&self, rowid: RowId) -> Option<T>
    where
        T: Clone,
    {
        self.column_data.get(rowid)
    }
}

impl<T> ColumnSegmentReader for GenericColumnSegmentReader<T> {
    fn doc_count(&self) -> usize {
        self.column_data.doc_count()
    }
}
