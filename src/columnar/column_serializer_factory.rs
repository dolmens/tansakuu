use crate::{
    schema::{DataType, Field},
    types::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};

use super::{
    ColumnSerializer, ListPrimitiveColumnSerializer, ListStringColumnSerializer,
    PrimitiveColumnSerializer, StringColumnSerializer,
};

#[derive(Default)]
pub struct ColumnSerializerFactory {}

impl ColumnSerializerFactory {
    pub fn create(&self, field: &Field) -> Box<dyn ColumnSerializer> {
        if !field.is_multi() {
            match field.data_type() {
                DataType::Str => Box::new(StringColumnSerializer::default()),

                DataType::Int8 => Box::new(PrimitiveColumnSerializer::<Int8Type>::default()),
                DataType::Int16 => Box::new(PrimitiveColumnSerializer::<Int16Type>::default()),
                DataType::Int32 => Box::new(PrimitiveColumnSerializer::<Int32Type>::default()),
                DataType::Int64 => Box::new(PrimitiveColumnSerializer::<Int64Type>::default()),
                DataType::UInt8 => Box::new(PrimitiveColumnSerializer::<UInt8Type>::default()),
                DataType::UInt16 => Box::new(PrimitiveColumnSerializer::<UInt16Type>::default()),
                DataType::UInt32 => Box::new(PrimitiveColumnSerializer::<UInt32Type>::default()),
                DataType::UInt64 => Box::new(PrimitiveColumnSerializer::<UInt64Type>::default()),

                DataType::Float32 => Box::new(PrimitiveColumnSerializer::<Float32Type>::default()),
                DataType::Float64 => Box::new(PrimitiveColumnSerializer::<Float64Type>::default()),
            }
        } else {
            match field.data_type() {
                DataType::Str => Box::new(ListStringColumnSerializer::default()),

                DataType::Int8 => Box::new(ListPrimitiveColumnSerializer::<Int8Type>::default()),
                DataType::Int16 => Box::new(ListPrimitiveColumnSerializer::<Int16Type>::default()),
                DataType::Int32 => Box::new(ListPrimitiveColumnSerializer::<Int32Type>::default()),
                DataType::Int64 => Box::new(ListPrimitiveColumnSerializer::<Int64Type>::default()),
                DataType::UInt8 => Box::new(ListPrimitiveColumnSerializer::<UInt8Type>::default()),
                DataType::UInt16 => {
                    Box::new(ListPrimitiveColumnSerializer::<UInt16Type>::default())
                }
                DataType::UInt32 => {
                    Box::new(ListPrimitiveColumnSerializer::<UInt32Type>::default())
                }
                DataType::UInt64 => {
                    Box::new(ListPrimitiveColumnSerializer::<UInt64Type>::default())
                }

                DataType::Float32 => {
                    Box::new(ListPrimitiveColumnSerializer::<Float32Type>::default())
                }
                DataType::Float64 => {
                    Box::new(ListPrimitiveColumnSerializer::<Float64Type>::default())
                }
            }
        }
    }
}
