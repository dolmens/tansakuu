use std::sync::Arc;

pub struct Schema {
    fields: Vec<Field>,
    indexes: Vec<Index>,
}

pub type SchemaRef = Arc<Schema>;

pub enum FieldType {
    Text,
}

pub struct Field {
    name: String,
    field_type: FieldType,
}

pub enum IndexType {
    Term,
    UniqueKey,
}

pub struct Index {
    name: String,
    index_type: IndexType,
    fields: Vec<String>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: vec![],
            indexes: vec![],
        }
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType) {
        self.fields.push(Field { name, field_type });
    }

    pub fn add_index(&mut self, name: String, index_type: IndexType, fields: Vec<String>) {
        self.indexes.push(Index {
            name,
            index_type,
            fields,
        });
    }

    pub fn indexes(&self) -> &[Index] {
        &self.indexes
    }

    pub fn indexes_of_field(&self, field: &str) -> &[Index] {
        &self.indexes
    }

    pub fn columns(&self) -> &[Field] {
        &self.fields
    }

    pub fn column_of_field(&self, name: &str) -> Option<&Field> {
        for field in &self.fields {
            if field.name == name {
                return Some(field);
            }
        }

        None
    }
}

impl Index {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index_type(&self) -> &IndexType {
        &self.index_type
    }
}

impl Field {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }
}
