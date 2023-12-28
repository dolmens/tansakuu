use std::sync::Arc;

use crate::RowId;

use super::GenericColumnSegmentData;

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
