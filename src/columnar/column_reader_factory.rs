use crate::{
    schema::{Field, FieldType},
    table::TableData,
    types::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};

use super::{
    multi_string_column_reader::MultiStringColumnReader, BooleanColumnReader, ColumnReader,
    GeoLocationColumnReader, MultiPrimitiveColumnReader, PrimitiveColumnReader, StringColumnReader,
};

#[derive(Default)]
pub struct ColumnReaderFactory {}

impl ColumnReaderFactory {
    pub fn create(&self, field: &Field, table_data: &TableData) -> Box<dyn ColumnReader> {
        if !field.is_multi() {
            match field.data_type() {
                FieldType::Str | FieldType::Text => {
                    Box::new(StringColumnReader::new(field, table_data))
                }

                FieldType::Boolean => Box::new(BooleanColumnReader::new(field, table_data)),

                FieldType::Int8 => {
                    Box::new(PrimitiveColumnReader::<Int8Type>::new(field, table_data))
                }
                FieldType::Int16 => {
                    Box::new(PrimitiveColumnReader::<Int16Type>::new(field, table_data))
                }
                FieldType::Int32 => {
                    Box::new(PrimitiveColumnReader::<Int32Type>::new(field, table_data))
                }
                FieldType::Int64 => {
                    Box::new(PrimitiveColumnReader::<Int64Type>::new(field, table_data))
                }
                FieldType::UInt8 => {
                    Box::new(PrimitiveColumnReader::<UInt8Type>::new(field, table_data))
                }
                FieldType::UInt16 => {
                    Box::new(PrimitiveColumnReader::<UInt16Type>::new(field, table_data))
                }
                FieldType::UInt32 => {
                    Box::new(PrimitiveColumnReader::<UInt32Type>::new(field, table_data))
                }
                FieldType::UInt64 => {
                    Box::new(PrimitiveColumnReader::<UInt64Type>::new(field, table_data))
                }

                FieldType::Float32 => {
                    Box::new(PrimitiveColumnReader::<Float32Type>::new(field, table_data))
                }
                FieldType::Float64 => {
                    Box::new(PrimitiveColumnReader::<Float64Type>::new(field, table_data))
                }

                FieldType::GeoLocation => Box::new(GeoLocationColumnReader::new(field, table_data)),
            }
        } else {
            match field.data_type() {
                FieldType::Str | FieldType::Text => {
                    Box::new(MultiStringColumnReader::new(field, table_data))
                }

                FieldType::Boolean => unimplemented!(),

                FieldType::Int8 => Box::new(MultiPrimitiveColumnReader::<Int8Type>::new(
                    field, table_data,
                )),
                FieldType::Int16 => Box::new(MultiPrimitiveColumnReader::<Int16Type>::new(
                    field, table_data,
                )),
                FieldType::Int32 => Box::new(MultiPrimitiveColumnReader::<Int32Type>::new(
                    field, table_data,
                )),
                FieldType::Int64 => Box::new(MultiPrimitiveColumnReader::<Int64Type>::new(
                    field, table_data,
                )),
                FieldType::UInt8 => Box::new(MultiPrimitiveColumnReader::<UInt8Type>::new(
                    field, table_data,
                )),
                FieldType::UInt16 => Box::new(MultiPrimitiveColumnReader::<UInt16Type>::new(
                    field, table_data,
                )),
                FieldType::UInt32 => Box::new(MultiPrimitiveColumnReader::<UInt32Type>::new(
                    field, table_data,
                )),
                FieldType::UInt64 => Box::new(MultiPrimitiveColumnReader::<UInt64Type>::new(
                    field, table_data,
                )),

                FieldType::Float32 => Box::new(MultiPrimitiveColumnReader::<Float32Type>::new(
                    field, table_data,
                )),
                FieldType::Float64 => Box::new(MultiPrimitiveColumnReader::<Float64Type>::new(
                    field, table_data,
                )),

                FieldType::GeoLocation => unimplemented!(),
            }
        }
    }
}
