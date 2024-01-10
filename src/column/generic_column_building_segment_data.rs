use crate::{util::ChunkedVec, DocId};

use super::ColumnSegmentData;

pub struct GenericColumnBuildingSegmentData<T> {
    pub values: ChunkedVec<T>,
}

impl<T> GenericColumnBuildingSegmentData<T> {
    pub fn new() -> Self {
        Self {
            values: ChunkedVec::new(4, 2),
        }
    }

    pub fn push(&self, value: T) {
        unsafe {
            self.values.push(value);
        }
    }

    pub fn get(&self, docid: DocId) -> Option<T>
    where
        T: Clone,
    {
        self.values.get(docid as usize).cloned()
    }

    pub fn values(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.values.iter().cloned().collect()
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

impl<T: Send + Sync + 'static> ColumnSegmentData for GenericColumnBuildingSegmentData<T> {}
