use allocator_api2::alloc::Global;

use crate::{DocId, util::chunked_vec::ChunkedVec};

use super::ColumnSegmentData;

pub struct GenericColumnBuildingSegmentData<T> {
    pub values: ChunkedVec<T, Global>,
}

impl<T> GenericColumnBuildingSegmentData<T> {
    pub fn new(values: ChunkedVec<T, Global>) -> Self {
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<T>
    where
        T: Clone,
    {
        self.values.get(docid as usize).cloned()
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

impl<T: Send + Sync + 'static> ColumnSegmentData for GenericColumnBuildingSegmentData<T> {}
