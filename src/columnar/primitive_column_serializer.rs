use std::{marker::PhantomData, path::Path, sync::Arc};

use arrow::array::{ArrayRef, PrimitiveArray};

use crate::{types::PrimitiveType, DocId};

use super::{
    column_serializer::ColumnSerializer, ColumnBuildingSegmentData,
    PrimitiveColumnBuildingSegmentData,
};

pub struct PrimitiveColumnSerializer<T: PrimitiveType> {
    _marker: PhantomData<T>,
}

impl<T: PrimitiveType> PrimitiveColumnSerializer<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T: PrimitiveType> Default for PrimitiveColumnSerializer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: PrimitiveType> ColumnSerializer for PrimitiveColumnSerializer<T> {
    fn serialize(&self, column_data: &dyn ColumnBuildingSegmentData) -> ArrayRef {
        let primitive_column_data = column_data
            .as_any()
            .downcast_ref::<PrimitiveColumnBuildingSegmentData<T::Native>>()
            .unwrap();

        let array = PrimitiveArray::<T::ArrowPrimitive>::from_iter_values(
            primitive_column_data.values.clone().into_iter(),
        );

        Arc::new(array)
    }
}
