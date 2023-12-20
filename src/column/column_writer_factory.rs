use crate::{
    column::GenericColumnWriter,
    schema::{Field, FieldType},
};

use super::ColumnWriter;

pub struct ColumnWriterFactory {}

impl ColumnWriterFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create(&self, field: &Field) -> Box<dyn ColumnWriter> {
        match field.field_type() {
            FieldType::Text => Box::new(GenericColumnWriter::<String>::new()),
        }
    }
}
