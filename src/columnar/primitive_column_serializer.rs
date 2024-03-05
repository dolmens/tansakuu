use std::{marker::PhantomData, sync::Arc};

use arrow::array::{ArrayRef, PrimitiveArray};

use crate::types::PrimitiveType;

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

impl<T: PrimitiveType> ColumnSerializer for PrimitiveColumnSerializer<T>
where
    PrimitiveArray<T::ArrowPrimitive>: From<Vec<Option<T::Native>>>,
{
    fn serialize(&self, column_data: &dyn ColumnBuildingSegmentData) -> ArrayRef {
        let primitive_column_data = column_data
            .as_any()
            .downcast_ref::<PrimitiveColumnBuildingSegmentData<T::Native>>()
            .unwrap();

        let values: Vec<_> = primitive_column_data.values.clone().into_iter().collect();
        let array: PrimitiveArray<T::ArrowPrimitive> = values.into();

        Arc::new(array)
    }
}
