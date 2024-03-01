use crate::schema::{DataType, Field};

use super::{ColumnWriter, PrimitiveColumnWriter, StringColumnWriter};

#[derive(Default)]
pub struct ColumnWriterFactory {}

impl ColumnWriterFactory {
    pub fn create(&self, field: &Field) -> Box<dyn ColumnWriter> {
        match field.data_type() {
            DataType::String => Box::new(StringColumnWriter::new()),
            DataType::Int64 => Box::new(PrimitiveColumnWriter::<i64>::new()),
        }
    }
}
