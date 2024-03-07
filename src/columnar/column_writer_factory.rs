use crate::schema::{DataType, FieldRef};

use super::{
    ColumnWriter, ListPrimitiveColumnWriter, ListStringColumnWriter, PrimitiveColumnWriter,
    StringColumnWriter,
};

#[derive(Default)]
pub struct ColumnWriterFactory {}

impl ColumnWriterFactory {
    pub fn create(&self, field: &FieldRef) -> Box<dyn ColumnWriter> {
        if !field.is_multi() {
            match field.data_type() {
                DataType::Str => Box::new(StringColumnWriter::new(field.clone())),

                DataType::Int8 => Box::new(PrimitiveColumnWriter::<i8>::new(field.clone())),
                DataType::Int16 => Box::new(PrimitiveColumnWriter::<i16>::new(field.clone())),
                DataType::Int32 => Box::new(PrimitiveColumnWriter::<i32>::new(field.clone())),
                DataType::Int64 => Box::new(PrimitiveColumnWriter::<i64>::new(field.clone())),
                DataType::UInt8 => Box::new(PrimitiveColumnWriter::<u8>::new(field.clone())),
                DataType::UInt16 => Box::new(PrimitiveColumnWriter::<u16>::new(field.clone())),
                DataType::UInt32 => Box::new(PrimitiveColumnWriter::<u32>::new(field.clone())),
                DataType::UInt64 => Box::new(PrimitiveColumnWriter::<u64>::new(field.clone())),

                DataType::Float32 => Box::new(PrimitiveColumnWriter::<f32>::new(field.clone())),
                DataType::Float64 => Box::new(PrimitiveColumnWriter::<f64>::new(field.clone())),
            }
        } else {
            match field.data_type() {
                DataType::Str => Box::new(ListStringColumnWriter::new(field.clone())),

                DataType::Int8 => Box::new(ListPrimitiveColumnWriter::<i8>::new(field.clone())),
                DataType::Int16 => Box::new(ListPrimitiveColumnWriter::<i16>::new(field.clone())),
                DataType::Int32 => Box::new(ListPrimitiveColumnWriter::<i32>::new(field.clone())),
                DataType::Int64 => Box::new(ListPrimitiveColumnWriter::<i64>::new(field.clone())),
                DataType::UInt8 => Box::new(ListPrimitiveColumnWriter::<u8>::new(field.clone())),
                DataType::UInt16 => Box::new(ListPrimitiveColumnWriter::<u16>::new(field.clone())),
                DataType::UInt32 => Box::new(ListPrimitiveColumnWriter::<u32>::new(field.clone())),
                DataType::UInt64 => Box::new(ListPrimitiveColumnWriter::<u64>::new(field.clone())),

                DataType::Float32 => Box::new(ListPrimitiveColumnWriter::<f32>::new(field.clone())),
                DataType::Float64 => Box::new(ListPrimitiveColumnWriter::<f64>::new(field.clone())),
            }
        }
    }
}
