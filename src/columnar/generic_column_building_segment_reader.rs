use std::sync::Arc;

use allocator_api2::alloc::Global;

use crate::{util::chunked_vec::ChunkedVec, DocId};

use super::GenericColumnBuildingSegmentData;

pub struct GenericColumnBuildingSegmentReader<T> {
    values: ChunkedVec<T, Global>,
}

impl<T: Clone> GenericColumnBuildingSegmentReader<T> {
    pub fn new(column_data: Arc<GenericColumnBuildingSegmentData<T>>) -> Self {
        let values = column_data.values.clone();
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<&T>
    where
        T: Clone,
    {
        self.values.get(docid as usize)
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}
