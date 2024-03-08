use super::{DataType, Schema};

#[derive(Debug, thiserror::Error)]
pub enum ArrowSchemaValidationError {
    #[error("Field count mismatch, expect: {0}, got: {1}")]
    FieldCountMismatch(usize, usize),
    #[error("Field mismatch, expect: {0}, got: {1}")]
    FieldMismatch(String, String),
    #[error("Field type mismatch, field type: {0}, multi: {1} arrow type: {2}")]
    FieldTypeMismatch(DataType, bool, arrow_schema::DataType),
}

#[derive(Default)]
pub struct ArrowSchemaValidator {}

impl ArrowSchemaValidator {
    pub fn validate(
        &self,
        schema: &Schema,
        arrow_schema: &arrow_schema::Schema,
    ) -> Result<(), ArrowSchemaValidationError> {
        if schema.columns().len() != arrow_schema.fields().len() {
            return Err(ArrowSchemaValidationError::FieldCountMismatch(
                schema.columns().len(),
                arrow_schema.fields().len(),
            ));
        }

        for (field, arrow_field) in schema.columns().iter().zip(arrow_schema.fields().iter()) {
            if field.name() != arrow_field.name() {
                return Err(ArrowSchemaValidationError::FieldMismatch(
                    field.name().to_string(),
                    arrow_field.name().clone(),
                ));
            }

            let arrow_field_type = if field.is_multi() {
                match arrow_field.data_type() {
                    arrow_schema::DataType::List(element_field) => element_field.data_type(),
                    _ => {
                        return Err(ArrowSchemaValidationError::FieldTypeMismatch(
                            field.data_type().clone(),
                            field.is_multi(),
                            arrow_field.data_type().clone(),
                        ));
                    }
                }
            } else {
                arrow_field.data_type()
            };

            if !field.is_multi() {
                let matched = match field.data_type() {
                    DataType::Str | DataType::Text => {
                        matches!(arrow_field_type, arrow_schema::DataType::Utf8)
                    }

                    DataType::Int8 => {
                        matches!(arrow_field_type, arrow_schema::DataType::Int8)
                    }
                    DataType::Int16 => {
                        matches!(arrow_field_type, arrow_schema::DataType::Int16)
                    }
                    DataType::Int32 => {
                        matches!(arrow_field_type, arrow_schema::DataType::Int32)
                    }

                    DataType::Int64 => {
                        matches!(arrow_field_type, arrow_schema::DataType::Int64)
                    }
                    DataType::UInt8 => {
                        matches!(arrow_field_type, arrow_schema::DataType::UInt8)
                    }
                    DataType::UInt16 => {
                        matches!(arrow_field_type, arrow_schema::DataType::UInt16)
                    }
                    DataType::UInt32 => {
                        matches!(arrow_field_type, arrow_schema::DataType::UInt32)
                    }
                    DataType::UInt64 => {
                        matches!(arrow_field_type, arrow_schema::DataType::UInt64)
                    }

                    DataType::Float32 => {
                        matches!(arrow_field_type, arrow_schema::DataType::Float32)
                    }
                    DataType::Float64 => {
                        matches!(arrow_field_type, arrow_schema::DataType::Float64)
                    }
                };
                if !matched {
                    return Err(ArrowSchemaValidationError::FieldTypeMismatch(
                        field.data_type().clone(),
                        field.is_multi(),
                        arrow_field.data_type().clone(),
                    ));
                }
            } else {
            }
        }

        Ok(())
    }
}
