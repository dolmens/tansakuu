use crate::{
    schema::{Field, FieldType},
    table::TableData,
};

use super::{ColumnReader, GenericColumnReader};

#[derive(Default)]
pub struct ColumnReaderFactory {}

impl ColumnReaderFactory {
    pub fn create(&self, field: &Field, table_data: &TableData) -> Box<dyn ColumnReader> {
        match field.field_type() {
            FieldType::Str => Box::new(GenericColumnReader::<String>::new(field, table_data)),
            FieldType::I64 => Box::new(GenericColumnReader::<i64>::new(field, table_data)),
        }
    }
}
