use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;

use crate::schema::FieldType;

use super::Schema;

#[derive(Default)]
pub struct SchemaConverter {}

impl SchemaConverter {
    pub fn convert_to_arrow(&self, schema: &Schema) -> ArrowSchema {
        let mut arrow_fields = Vec::with_capacity(schema.columns().len());
        for field in schema.columns() {
            if !field.is_multi() {
                let arrow_field = match field.data_type() {
                    FieldType::Str | FieldType::Text => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Utf8,
                        field.is_nullable(),
                    ),

                    FieldType::Int8 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int8,
                        field.is_nullable(),
                    ),
                    FieldType::Int16 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int16,
                        field.is_nullable(),
                    ),
                    FieldType::Int32 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int32,
                        field.is_nullable(),
                    ),
                    FieldType::Int64 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int64,
                        field.is_nullable(),
                    ),
                    FieldType::UInt8 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::UInt8,
                        field.is_nullable(),
                    ),
                    FieldType::UInt16 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::UInt16,
                        field.is_nullable(),
                    ),
                    FieldType::UInt32 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::UInt32,
                        field.is_nullable(),
                    ),
                    FieldType::UInt64 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::UInt64,
                        field.is_nullable(),
                    ),

                    FieldType::Float32 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Float32,
                        field.is_nullable(),
                    ),
                    FieldType::Float64 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Float64,
                        field.is_nullable(),
                    ),
                };
                arrow_fields.push(arrow_field);
            } else {
                let arrow_field = match field.data_type() {
                    FieldType::Str | FieldType::Text => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Utf8, true)
                    }

                    FieldType::Int8 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int8, true)
                    }
                    FieldType::Int16 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int16, true)
                    }
                    FieldType::Int32 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int32, true)
                    }
                    FieldType::Int64 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int64, true)
                    }
                    FieldType::UInt8 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::UInt8, true)
                    }
                    FieldType::UInt16 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::UInt16, true)
                    }
                    FieldType::UInt32 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::UInt32, true)
                    }
                    FieldType::UInt64 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::UInt64, true)
                    }

                    FieldType::Float32 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Float32, true)
                    }
                    FieldType::Float64 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Float64, true)
                    }
                };
                let arrow_list_field = arrow_schema::Field::new(
                    field.name(),
                    arrow_schema::DataType::List(Arc::new(arrow_field)),
                    field.is_nullable(),
                );
                arrow_fields.push(arrow_list_field);
            }
        }
        ArrowSchema::new(arrow_fields)
    }
}
