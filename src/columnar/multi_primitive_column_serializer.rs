use std::{marker::PhantomData, sync::Arc};

use arrow::array::{ArrayRef, ListBuilder, PrimitiveBuilder};

use crate::types::PrimitiveType;

use super::{
    column_serializer::ColumnSerializer, ColumnBuildingSegmentData,
    MultiPrimitiveColumnBuildingSegmentData,
};

pub struct MultiPrimitiveColumnSerializer<T: PrimitiveType> {
    _marker: PhantomData<T>,
}

impl<T: PrimitiveType> MultiPrimitiveColumnSerializer<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T: PrimitiveType> Default for MultiPrimitiveColumnSerializer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: PrimitiveType> ColumnSerializer for MultiPrimitiveColumnSerializer<T> {
    fn serialize(
        &self,
        column_data: &dyn ColumnBuildingSegmentData,
        _doc_count: usize,
        _docid_mapping: Option<&Vec<Option<crate::DocId>>>,
    ) -> ArrayRef {
        let list_primitive_column_data = column_data
            .as_any()
            .downcast_ref::<MultiPrimitiveColumnBuildingSegmentData<T::Native>>()
            .unwrap();

        let values = list_primitive_column_data.values.iter();

        let values_builder = PrimitiveBuilder::<T::ArrowPrimitive>::new();
        let mut builder = ListBuilder::new(values_builder);
        for primitive_vec in values {
            if let Some(vec) = primitive_vec {
                for &v in vec.iter() {
                    builder.values().append_value(v);
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        let array = builder.finish();
        Arc::new(array)
    }
}
