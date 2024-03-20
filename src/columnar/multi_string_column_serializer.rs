use std::sync::Arc;

use arrow::array::{ArrayRef, ListBuilder, StringBuilder};

use super::{
    column_serializer::ColumnSerializer, ColumnBuildingSegmentData,
    MultiStringColumnBuildingSegmentData,
};

#[derive(Default)]
pub struct MultiStringColumnSerializer {}

impl ColumnSerializer for MultiStringColumnSerializer {
    fn serialize(&self, column_data: &dyn ColumnBuildingSegmentData) -> ArrayRef {
        let list_string_column_data = column_data
            .as_any()
            .downcast_ref::<MultiStringColumnBuildingSegmentData>()
            .unwrap();

        let values = list_string_column_data.values.iter();

        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);

        for string_vec in values {
            if let Some(v) = string_vec {
                for s in v.iter() {
                    builder.values().append_value(s.as_str());
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
