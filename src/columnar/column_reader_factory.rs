use crate::{
    schema::{Field, FieldType},
    table::TableData,
    types::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};

use super::{
    list_string_column_reader::ListStringColumnReader, ColumnReader, ListPrimitiveColumnReader,
    PrimitiveColumnReader, StringColumnReader,
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
            }
        } else {
            match field.data_type() {
                FieldType::Str | FieldType::Text => {
                    Box::new(ListStringColumnReader::new(field, table_data))
                }

                FieldType::Int8 => Box::new(ListPrimitiveColumnReader::<Int8Type>::new(
                    field, table_data,
                )),
                FieldType::Int16 => Box::new(ListPrimitiveColumnReader::<Int16Type>::new(
                    field, table_data,
                )),
                FieldType::Int32 => Box::new(ListPrimitiveColumnReader::<Int32Type>::new(
                    field, table_data,
                )),
                FieldType::Int64 => Box::new(ListPrimitiveColumnReader::<Int64Type>::new(
                    field, table_data,
                )),
                FieldType::UInt8 => Box::new(ListPrimitiveColumnReader::<UInt8Type>::new(
                    field, table_data,
                )),
                FieldType::UInt16 => Box::new(ListPrimitiveColumnReader::<UInt16Type>::new(
                    field, table_data,
                )),
                FieldType::UInt32 => Box::new(ListPrimitiveColumnReader::<UInt32Type>::new(
                    field, table_data,
                )),
                FieldType::UInt64 => Box::new(ListPrimitiveColumnReader::<UInt64Type>::new(
                    field, table_data,
                )),

                FieldType::Float32 => Box::new(ListPrimitiveColumnReader::<Float32Type>::new(
                    field, table_data,
                )),
                FieldType::Float64 => Box::new(ListPrimitiveColumnReader::<Float64Type>::new(
                    field, table_data,
                )),
            }
        }
    }
}
