use crate::{
    schema::{DataType, Field},
    table::TableData,
    types::Int64Type,
};

use super::{ColumnReader, PrimitiveColumnReader, StringColumnReader};

#[derive(Default)]
pub struct ColumnReaderFactory {}

impl ColumnReaderFactory {
    pub fn create(&self, field: &Field, table_data: &TableData) -> Box<dyn ColumnReader> {
        match field.data_type() {
            DataType::String => Box::new(StringColumnReader::new(field, table_data)),
            DataType::Int64 => Box::new(PrimitiveColumnReader::<Int64Type>::new(field, table_data)),
        }
    }
}
