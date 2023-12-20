use crate::{
    schema::{Field, FieldType},
    table::TableData,
};

use super::{ColumnReader, GenericColumnReader};

pub struct ColumnReaderFactory {}

impl ColumnReaderFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create(&self, field: &Field, table_data: &TableData) -> Box<dyn ColumnReader> {
        match field.field_type() {
            FieldType::Text => Box::new(GenericColumnReader::<String>::new(field, table_data)),
        }
    }
}
