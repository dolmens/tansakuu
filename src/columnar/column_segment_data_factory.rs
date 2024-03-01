use crate::schema::{DataType, Field};

use super::ColumnSegmentDataBuilder;

#[derive(Default)]
pub struct ColumnSegmentDataFactory {}

impl ColumnSegmentDataFactory {
    pub fn create_builder(&self, field: &Field) -> Box<dyn ColumnSegmentDataBuilder> {
        unimplemented!()
        // match field.data_type() {
        //     DataType::String => Box::new(GenericColumnSegmentDataBuilder::<String>::new()),
        //     DataType::Int64 => Box::new(GenericColumnSegmentDataBuilder::<i64>::new()),
        // }
    }
}
