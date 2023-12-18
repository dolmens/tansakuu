use std::sync::Arc;

pub struct Schema {
    fields: Vec<Field>,
    indexes: Vec<Index>,
    attributes: Vec<Attribute>,
}

pub type SchemaRef = Arc<Schema>;

pub struct Field {
    name: String,
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

pub struct Attribute {
    name: String,
    field: String,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: vec![],
            indexes: vec![],
            attributes: vec![],
        }
    }

    pub fn indexes(&self) -> &[Index] {
        &self.indexes
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
