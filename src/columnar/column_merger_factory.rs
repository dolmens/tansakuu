use crate::{
    schema::{DataType, Field},
    types::Int64Type,
};

use super::{ColumnMerger, PrimitiveColumnMerger, StringColumnMerger};

#[derive(Default)]
pub struct ColumnMergerFactory {}

impl ColumnMergerFactory {
    pub fn create(&self, field: &Field) -> Box<dyn ColumnMerger> {
        match field.data_type() {
            DataType::String => Box::new(StringColumnMerger::default()),
            DataType::Int64 => Box::new(PrimitiveColumnMerger::<Int64Type>::default()),
        }
    }
}
