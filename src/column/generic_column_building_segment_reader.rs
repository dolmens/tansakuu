use std::sync::Arc;

use crate::RowId;

use super::{ColumnSegmentReader, GenericColumnBuildingSegmentData};

pub struct GenericColumnBuildingSegmentReader<T> {
    column_data: Arc<GenericColumnBuildingSegmentData<T>>,
}

impl<T> GenericColumnBuildingSegmentReader<T> {
    pub fn new(column_data: Arc<GenericColumnBuildingSegmentData<T>>) -> Self {
        Self { column_data }
    }

    pub fn get(&self, rowid: RowId) -> Option<T>
    where
        T: Clone,
    {
        self.column_data.get(rowid)
    }
}

impl<T> ColumnSegmentReader for GenericColumnBuildingSegmentReader<T> {
    fn doc_count(&self) -> usize {
        self.column_data.doc_count()
    }
}
