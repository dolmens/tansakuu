use crate::{
    schema::{Field, FieldType},
    types::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};

use super::{
    boolean_column_serializer::BooleanColumnSerializer, ColumnSerializer,
    GeoLocationColumnSerializer, MultiPrimitiveColumnSerializer, MultiStringColumnSerializer,
    PrimitiveColumnSerializer, StringColumnSerializer,
};

#[derive(Default)]
pub struct ColumnSerializerFactory {}

impl ColumnSerializerFactory {
    pub fn create(&self, field: &Field) -> Box<dyn ColumnSerializer> {
        if !field.is_multi() {
            match field.data_type() {
                FieldType::Str | FieldType::Text => Box::new(StringColumnSerializer::default()),

                FieldType::Boolean => Box::new(BooleanColumnSerializer::default()),

                FieldType::Int8 => Box::new(PrimitiveColumnSerializer::<Int8Type>::default()),
                FieldType::Int16 => Box::new(PrimitiveColumnSerializer::<Int16Type>::default()),
                FieldType::Int32 => Box::new(PrimitiveColumnSerializer::<Int32Type>::default()),
                FieldType::Int64 => Box::new(PrimitiveColumnSerializer::<Int64Type>::default()),
                FieldType::UInt8 => Box::new(PrimitiveColumnSerializer::<UInt8Type>::default()),
                FieldType::UInt16 => Box::new(PrimitiveColumnSerializer::<UInt16Type>::default()),
                FieldType::UInt32 => Box::new(PrimitiveColumnSerializer::<UInt32Type>::default()),
                FieldType::UInt64 => Box::new(PrimitiveColumnSerializer::<UInt64Type>::default()),

                FieldType::Float32 => Box::new(PrimitiveColumnSerializer::<Float32Type>::default()),
                FieldType::Float64 => Box::new(PrimitiveColumnSerializer::<Float64Type>::default()),

                FieldType::GeoLocation => Box::new(GeoLocationColumnSerializer::default()),
            }
        } else {
            match field.data_type() {
                FieldType::Str | FieldType::Text => {
                    Box::new(MultiStringColumnSerializer::default())
                }

                FieldType::Boolean => unimplemented!(),

                FieldType::Int8 => Box::new(MultiPrimitiveColumnSerializer::<Int8Type>::default()),
                FieldType::Int16 => {
                    Box::new(MultiPrimitiveColumnSerializer::<Int16Type>::default())
                }
                FieldType::Int32 => {
                    Box::new(MultiPrimitiveColumnSerializer::<Int32Type>::default())
                }
                FieldType::Int64 => {
                    Box::new(MultiPrimitiveColumnSerializer::<Int64Type>::default())
                }
                FieldType::UInt8 => {
                    Box::new(MultiPrimitiveColumnSerializer::<UInt8Type>::default())
                }
                FieldType::UInt16 => {
                    Box::new(MultiPrimitiveColumnSerializer::<UInt16Type>::default())
                }
                FieldType::UInt32 => {
                    Box::new(MultiPrimitiveColumnSerializer::<UInt32Type>::default())
                }
                FieldType::UInt64 => {
                    Box::new(MultiPrimitiveColumnSerializer::<UInt64Type>::default())
                }

                FieldType::Float32 => {
                    Box::new(MultiPrimitiveColumnSerializer::<Float32Type>::default())
                }
                FieldType::Float64 => {
                    Box::new(MultiPrimitiveColumnSerializer::<Float64Type>::default())
                }

                FieldType::GeoLocation => unimplemented!(),
            }
        }
    }
}
