use std::{marker::PhantomData, path::Path, sync::Arc};

use arrow::array::{ArrayRef, PrimitiveArray, StringArray};

use crate::{types::PrimitiveType, DocId};

use super::{
    column_serializer::ColumnSerializer, ColumnBuildingSegmentData, StringColumnBuildingSegmentData,
};

#[derive(Default)]
pub struct StringColumnSerializer {}

impl ColumnSerializer for StringColumnSerializer {
    fn serialize(&self, column_data: &dyn ColumnBuildingSegmentData) -> ArrayRef {
        let string_column_data = column_data
            .as_any()
            .downcast_ref::<StringColumnBuildingSegmentData>()
            .unwrap();

        let array = string_column_data
            .values
            .iter()
            .map(|s| Some(s.as_str()))
            .collect::<StringArray>();

        Arc::new(array)
    }
}
