use crate::schema::{FieldRef, FieldType};

use super::{
    boolean_column_writer::BooleanColumnWriter, ColumnWriter, GeoLocationColumnWriter,
    MultiPrimitiveColumnWriter, MultiStringColumnWriter, PrimitiveColumnWriter, StringColumnWriter,
};

#[derive(Default)]
pub struct ColumnWriterFactory {}

impl ColumnWriterFactory {
    pub fn create(&self, field: &FieldRef) -> Box<dyn ColumnWriter> {
        if !field.is_multi() {
            match field.data_type() {
                FieldType::Str | FieldType::Text => {
                    Box::new(StringColumnWriter::new(field.clone()))
                }

                FieldType::Boolean => Box::new(BooleanColumnWriter::new(field.clone())),

                FieldType::Int8 => Box::new(PrimitiveColumnWriter::<i8>::new(field.clone())),
                FieldType::Int16 => Box::new(PrimitiveColumnWriter::<i16>::new(field.clone())),
                FieldType::Int32 => Box::new(PrimitiveColumnWriter::<i32>::new(field.clone())),
                FieldType::Int64 => Box::new(PrimitiveColumnWriter::<i64>::new(field.clone())),
                FieldType::UInt8 => Box::new(PrimitiveColumnWriter::<u8>::new(field.clone())),
                FieldType::UInt16 => Box::new(PrimitiveColumnWriter::<u16>::new(field.clone())),
                FieldType::UInt32 => Box::new(PrimitiveColumnWriter::<u32>::new(field.clone())),
                FieldType::UInt64 => Box::new(PrimitiveColumnWriter::<u64>::new(field.clone())),

                FieldType::Float32 => Box::new(PrimitiveColumnWriter::<f32>::new(field.clone())),
                FieldType::Float64 => Box::new(PrimitiveColumnWriter::<f64>::new(field.clone())),

                FieldType::GeoLocation => Box::new(GeoLocationColumnWriter::new(field.clone())),
            }
        } else {
            match field.data_type() {
                FieldType::Str | FieldType::Text => {
                    Box::new(MultiStringColumnWriter::new(field.clone()))
                }

                FieldType::Boolean => unimplemented!(),

                FieldType::Int8 => Box::new(MultiPrimitiveColumnWriter::<i8>::new(field.clone())),
                FieldType::Int16 => Box::new(MultiPrimitiveColumnWriter::<i16>::new(field.clone())),
                FieldType::Int32 => Box::new(MultiPrimitiveColumnWriter::<i32>::new(field.clone())),
                FieldType::Int64 => Box::new(MultiPrimitiveColumnWriter::<i64>::new(field.clone())),
                FieldType::UInt8 => Box::new(MultiPrimitiveColumnWriter::<u8>::new(field.clone())),
                FieldType::UInt16 => {
                    Box::new(MultiPrimitiveColumnWriter::<u16>::new(field.clone()))
                }
                FieldType::UInt32 => {
                    Box::new(MultiPrimitiveColumnWriter::<u32>::new(field.clone()))
                }
                FieldType::UInt64 => {
                    Box::new(MultiPrimitiveColumnWriter::<u64>::new(field.clone()))
                }

                FieldType::Float32 => {
                    Box::new(MultiPrimitiveColumnWriter::<f32>::new(field.clone()))
                }
                FieldType::Float64 => {
                    Box::new(MultiPrimitiveColumnWriter::<f64>::new(field.clone()))
                }

                FieldType::GeoLocation => unimplemented!(),
            }
        }
    }
}
