use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;

use crate::schema::DataType;

use super::Schema;

#[derive(Default)]
pub struct SchemaConverter {}

impl SchemaConverter {
    pub fn convert_to_arrow(&self, schema: &Schema) -> ArrowSchema {
        let mut arrow_fields = Vec::with_capacity(schema.columns().len());
        for field in schema.columns() {
            if !field.is_multi() {
                let arrow_field = match field.data_type() {
                    DataType::String => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Utf8,
                        field.is_nullable(),
                    ),

                    DataType::Int8 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int8,
                        field.is_nullable(),
                    ),
                    DataType::Int16 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int16,
                        field.is_nullable(),
                    ),
                    DataType::Int32 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int32,
                        field.is_nullable(),
                    ),
                    DataType::Int64 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int64,
                        field.is_nullable(),
                    ),
                    DataType::UInt8 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::UInt8,
                        field.is_nullable(),
                    ),
                    DataType::UInt16 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::UInt16,
                        field.is_nullable(),
                    ),
                    DataType::UInt32 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::UInt32,
                        field.is_nullable(),
                    ),
                    DataType::UInt64 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::UInt64,
                        field.is_nullable(),
                    ),

                    DataType::Float32 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Float32,
                        field.is_nullable(),
                    ),
                    DataType::Float64 => arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Float64,
                        field.is_nullable(),
                    ),
                };
                arrow_fields.push(arrow_field);
            } else {
                let arrow_field = match field.data_type() {
                    DataType::String => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Utf8, true)
                    }

                    DataType::Int8 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int8, true)
                    }
                    DataType::Int16 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int16, true)
                    }
                    DataType::Int32 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int32, true)
                    }
                    DataType::Int64 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Int64, true)
                    }
                    DataType::UInt8 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::UInt8, true)
                    }
                    DataType::UInt16 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::UInt16, true)
                    }
                    DataType::UInt32 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::UInt32, true)
                    }
                    DataType::UInt64 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::UInt64, true)
                    }

                    DataType::Float32 => {
                        arrow_schema::Field::new("item", arrow_schema::DataType::Float32, true)
                    }
                    DataType::Float64 => {
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
