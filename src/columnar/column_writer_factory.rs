use crate::{
    columnar::GenericColumnWriter,
    schema::{Field, FieldType},
};

use super::ColumnWriter;

#[derive(Default)]
pub struct ColumnWriterFactory {}

impl ColumnWriterFactory {
    pub fn create(&self, field: &Field) -> Box<dyn ColumnWriter> {
        match field.field_type() {
            FieldType::Str => Box::new(GenericColumnWriter::<String>::new()),
            FieldType::I64 => Box::new(GenericColumnWriter::<i64>::new()),
        }
    }
}
