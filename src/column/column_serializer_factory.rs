use std::sync::Arc;

use crate::schema::{Field, FieldType};

use super::{
    ColumnSegmentData, ColumnSerializer, GenericColumnBuildingSegmentData, GenericColumnSerializer,
};

pub struct ColumnSerializerFactory {}

impl ColumnSerializerFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create(
        &self,
        field: &Field,
        column_data: Arc<dyn ColumnSegmentData>,
    ) -> Box<dyn ColumnSerializer> {
        match field.field_type() {
            FieldType::Text => {
                let generic_column_data = column_data
                    .downcast_arc::<GenericColumnBuildingSegmentData<String>>()
                    .ok()
                    .unwrap();
                Box::new(GenericColumnSerializer::new(field, generic_column_data))
            }
        }
    }
}
