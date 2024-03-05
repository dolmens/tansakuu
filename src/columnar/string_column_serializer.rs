use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};

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

        let array = string_column_data.values.iter().collect::<StringArray>();
        Arc::new(array)
    }
}
