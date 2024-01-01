use std::sync::Arc;

use crate::DocId;

use super::GenericColumnBuildingSegmentData;

pub struct GenericColumnBuildingSegmentReader<T> {
    column_data: Arc<GenericColumnBuildingSegmentData<T>>,
}

impl<T> GenericColumnBuildingSegmentReader<T> {
    pub fn new(column_data: Arc<GenericColumnBuildingSegmentData<T>>) -> Self {
        Self { column_data }
    }

    pub fn get(&self, docid: DocId) -> Option<T>
    where
        T: Clone,
    {
        self.column_data.get(docid)
    }

    pub fn doc_count(&self) -> usize {
        self.column_data.doc_count()
    }
}
