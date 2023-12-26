use std::sync::Arc;

use crate::schema::{Field, FieldType};

use super::{
    ColumnSegmentData, ColumnSerializer, GenericColumnBuildingSegmentData, GenericColumnSerializer,
};

#[derive(Default)]
pub struct ColumnSerializerFactory {}

impl ColumnSerializerFactory {
    pub fn create(
        &self,
        field: &Field,
        column_data: Arc<dyn ColumnSegmentData>,
    ) -> Box<dyn ColumnSerializer> {
        match field.field_type() {
            FieldType::Str => {
                let generic_column_data = column_data
                    .downcast_arc::<GenericColumnBuildingSegmentData<String>>()
                    .ok()
                    .unwrap();
                Box::new(GenericColumnSerializer::new(field, generic_column_data))
            }
            FieldType::I64 => {
                let generic_column_data = column_data
                    .downcast_arc::<GenericColumnBuildingSegmentData<i64>>()
                    .ok()
                    .unwrap();
                Box::new(GenericColumnSerializer::new(field, generic_column_data))
            }
        }
    }
}
