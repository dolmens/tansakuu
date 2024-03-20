use crate::{types::NativeType, util::chunked_vec::ChunkedVec};

use super::ColumnBuildingSegmentData;

pub struct MultiPrimitiveColumnBuildingSegmentData<T: NativeType> {
    pub values: ChunkedVec<Option<Box<[T]>>>,
}

impl<T: NativeType> MultiPrimitiveColumnBuildingSegmentData<T> {
    pub fn new(values: ChunkedVec<Option<Box<[T]>>>) -> Self {
        Self { values }
    }
}

impl<T: NativeType> ColumnBuildingSegmentData for MultiPrimitiveColumnBuildingSegmentData<T> {}
