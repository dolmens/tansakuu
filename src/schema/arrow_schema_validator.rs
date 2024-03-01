use super::{DataType, Schema};

#[derive(Default)]
pub struct ArrowSchemaValidator {}

impl ArrowSchemaValidator {
    pub fn validate(&self, schema: &Schema, arrow_schema: &arrow_schema::Schema) -> bool {
        let columns: Vec<_> = schema.columns().collect();
        if columns.len() != arrow_schema.fields().len() {
            return false;
        }

        for (&field, &arrow_field) in columns.iter().zip(arrow_schema.all_fields().iter()) {
            if field.name() != arrow_field.name() {
                return false;
            }
            match field.data_type() {
                DataType::String => {
                    if !matches!(arrow_field.data_type(), arrow_schema::DataType::Utf8) {
                        return false;
                    }
                }
                DataType::Int64 => {
                    if !matches!(arrow_field.data_type(), arrow_schema::DataType::Int64) {
                        return false;
                    }
                }
            }
        }

        true
    }
}
