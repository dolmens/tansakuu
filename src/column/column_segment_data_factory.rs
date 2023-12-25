use crate::schema::{Field, FieldType};

use super::{ColumnSegmentDataBuilder, GenericColumnSegmentDataBuilder};

#[derive(Default)]
pub struct ColumnSegmentDataFactory {}

impl ColumnSegmentDataFactory {
    pub fn create_builder(&self, field: &Field) -> Box<dyn ColumnSegmentDataBuilder> {
        match field.field_type() {
            FieldType::Text => Box::new(GenericColumnSegmentDataBuilder::<String>::new()),
        }
    }
}
