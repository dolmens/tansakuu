use std::sync::Arc;

use crate::{types::NativeType, util::chunked_vec::ChunkedVec, DocId};

use super::PrimitiveColumnBuildingSegmentData;

pub struct PrimitiveColumnBuildingSegmentReader<T: NativeType> {
    values: ChunkedVec<Option<T>>,
}

impl<T: NativeType> PrimitiveColumnBuildingSegmentReader<T> {
    pub fn new(column_data: Arc<PrimitiveColumnBuildingSegmentData<T>>) -> Self {
        let values = column_data.values.clone();
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<T> {
        self.values.get(docid as usize).unwrap().as_ref().copied()
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}
