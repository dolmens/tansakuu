use crate::schema::{Field, FieldType};

use super::{ColumnMerger, GenericColumnMerger};

#[derive(Default)]
pub struct ColumnMergerFactory {}

impl ColumnMergerFactory {
    pub fn create(&self, field: &Field) -> Box<dyn ColumnMerger> {
        match field.field_type() {
            FieldType::Text => Box::new(GenericColumnMerger::<String>::default()),
        }
    }
}
