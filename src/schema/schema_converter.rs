use arrow_schema::Schema as ArrowSchema;

use crate::schema::DataType;

use super::Schema;

#[derive(Default)]
pub struct SchemaConverter {}

impl SchemaConverter {
    pub fn convert_to_arrow(&self, schema: &Schema) -> ArrowSchema {
        let mut arrow_fields = Vec::with_capacity(schema.columns().len());
        for field in schema.columns() {
            match field.data_type() {
                DataType::String => {
                    let arrow_field =
                        arrow_schema::Field::new(field.name(), arrow_schema::DataType::Utf8, false);
                    arrow_fields.push(arrow_field);
                }
                DataType::Int64 => {
                    let arrow_field = arrow_schema::Field::new(
                        field.name(),
                        arrow_schema::DataType::Int64,
                        false,
                    );
                    arrow_fields.push(arrow_field);
                }
            }
        }
        ArrowSchema::new(arrow_fields)
    }
}
