use crate::{
    schema::{DataType, Field},
    types::Int64Type,
};

use super::{ColumnSerializer, PrimitiveColumnSerializer, StringColumnSerializer};

#[derive(Default)]
pub struct ColumnSerializerFactory {}

impl ColumnSerializerFactory {
    pub fn create(&self, field: &Field) -> Box<dyn ColumnSerializer> {
        match field.data_type() {
            DataType::String => Box::new(StringColumnSerializer::default()),
            DataType::Int64 => Box::new(PrimitiveColumnSerializer::<Int64Type>::default()),
        }
    }
}
