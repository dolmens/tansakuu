use crate::{types::NativeType, util::chunked_vec::ChunkedVec};

use super::ColumnBuildingSegmentData;

pub struct ListPrimitiveColumnBuildingSegmentData<T: NativeType> {
    pub values: ChunkedVec<Option<Box<[T]>>>,
}

impl<T: NativeType> ListPrimitiveColumnBuildingSegmentData<T> {
    pub fn new(values: ChunkedVec<Option<Box<[T]>>>) -> Self {
        Self { values }
    }
}

impl<T: NativeType> ColumnBuildingSegmentData for ListPrimitiveColumnBuildingSegmentData<T> {}
