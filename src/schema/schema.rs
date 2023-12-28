use std::{collections::HashMap, ops::BitOr, sync::Arc};

#[derive(Default)]
pub struct SchemaBuilder {
    schema: Schema,
}

#[derive(Default)]
pub struct Schema {
    fields: Vec<Field>,
    indexes: Vec<Index>,
    primary_key: Option<(String, String)>,
    fields_map: HashMap<String, FieldEntry>,
    indexes_map: HashMap<String, IndexEntry>,
}

pub type SchemaRef = Arc<Schema>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldType {
    Str,
    I64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FieldEntry(usize);

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    name: String,
    field_type: FieldType,
    column: bool,
    indexes: Vec<IndexEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexType {
    Term,
    PrimaryKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Index {
    name: String,
    index_type: IndexType,
    fields: Vec<FieldEntry>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FieldOptions {
    column: bool,
    indexed: bool,
    primary_key: bool,
}

pub const COLUMN: FieldOptions = FieldOptions {
    column: true,
    indexed: false,
    primary_key: false,
};

pub const INDEXED: FieldOptions = FieldOptions {
    column: false,
    indexed: true,
    primary_key: false,
};

pub const PRIMARY_KEY: FieldOptions = FieldOptions {
    column: false,
    indexed: false,
    primary_key: true,
};

impl BitOr for FieldOptions {
    type Output = FieldOptions;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self {
            column: self.column || rhs.column,
            indexed: self.indexed || rhs.indexed,
            primary_key: self.primary_key || rhs.primary_key,
        }
    }
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_text_field(&mut self, field_name: String, options: FieldOptions) {
        self.add_field(field_name, FieldType::Str, options);
    }

    pub fn add_i64_field(&mut self, field_name: String, options: FieldOptions) {
        self.add_field(field_name, FieldType::I64, options);
    }

    pub fn add_field(&mut self, field_name: String, field_type: FieldType, options: FieldOptions) {
        let mut options = options;
        if options.primary_key {
            options.column = true;
            options.indexed = true;
        }
        assert!(!self.schema.fields_map.contains_key(&field_name));
        let field = Field {
            name: field_name.clone(),
            field_type,
            column: options.column,
            indexes: vec![],
        };
        self.schema
            .fields_map
            .insert(field_name.clone(), FieldEntry(self.schema.fields.len()));
        self.schema.fields.push(field);

        if options.indexed {
            let fields = vec![field_name.clone()];
            let index_type = if options.primary_key {
                IndexType::PrimaryKey
            } else {
                IndexType::Term
            };
            self.add_index(field_name, index_type, &fields);
        }
    }

    pub fn add_index(&mut self, index_name: String, index_type: IndexType, fields: &[String]) {
        assert!(!self.schema.indexes_map.contains_key(&index_name));
        if index_type == IndexType::PrimaryKey {
            assert!(self.schema.primary_key.is_none());
            assert_eq!(fields.len(), 1);
            self.schema.primary_key = Some((fields[0].clone(), index_name.clone()));
        }
        let field_entries: Vec<_> = fields
            .iter()
            .map(|f| self.schema.fields_map.get(f).unwrap())
            .cloned()
            .collect();
        for entry in &field_entries {
            self.schema.fields[entry.0]
                .indexes
                .push(IndexEntry(self.schema.indexes.len()));
        }
        let index = Index {
            name: index_name.clone(),
            index_type,
            fields: field_entries,
        };
        self.schema
            .indexes_map
            .insert(index_name, IndexEntry(self.schema.indexes.len()));
        self.schema.indexes.push(index);
    }

    pub fn build(self) -> Schema {
        self.schema
    }
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    pub fn field(&self, field_name: &str) -> Option<&Field> {
        self.fields_map
            .get(field_name)
            .map(|&entry| &self.fields[entry.0])
    }

    pub fn index(&self, index_name: &str) -> Option<&Index> {
        self.indexes_map
            .get(index_name)
            .map(|&entry| &self.indexes[entry.0])
    }

    pub fn primary_key(&self) -> Option<&(String, String)> {
        self.primary_key.as_ref()
    }

    pub fn indexes_of_field<'a>(&'a self, field: &'a Field) -> impl Iterator<Item = &Index> + 'a {
        field.indexes.iter().map(|&i| &self.indexes[i.0])
    }

    pub fn fields_of_index<'a>(&'a self, index: &'a Index) -> impl Iterator<Item = &Field> + 'a {
        index.fields.iter().map(|&i| &self.fields[i.0])
    }

    pub fn indexes(&self) -> &[Index] {
        &self.indexes
    }

    pub fn columns(&self) -> &[Field] {
        &self.fields
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

    pub fn is_column(&self) -> bool {
        self.column
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{
        schema::{FieldEntry, FieldOptions, IndexEntry, COLUMN, INDEXED},
        Field, FieldType, Index, IndexType,
    };

    use super::SchemaBuilder;

    #[test]
    fn field_options_bitor() {
        assert_eq!(
            COLUMN | INDEXED,
            FieldOptions {
                column: true,
                indexed: true,
                primary_key: false,
            }
        );
    }

    #[test]
    fn schema_builder() {
        let mut builder = SchemaBuilder::new();
        // column and indexed
        builder.add_text_field("f1".to_string(), COLUMN | INDEXED);
        assert_eq!(
            builder.schema.fields[0],
            Field {
                name: "f1".to_string(),
                field_type: FieldType::Str,
                column: true,
                indexes: vec![IndexEntry(0)],
            }
        );
        assert_eq!(
            builder.schema.indexes[0],
            Index {
                name: "f1".to_string(),
                index_type: IndexType::Term,
                fields: vec![FieldEntry(0)],
            }
        );
        assert_eq!(builder.schema.fields_map.get("f1"), Some(&FieldEntry(0)));
        assert_eq!(builder.schema.indexes_map.get("f1"), Some(&IndexEntry(0)));

        // only column
        builder.add_text_field("f2".to_string(), COLUMN);
        assert_eq!(
            builder.schema.fields[1],
            Field {
                name: "f2".to_string(),
                field_type: FieldType::Str,
                column: true,
                indexes: vec![],
            }
        );
        assert_eq!(builder.schema.fields_map.get("f2"), Some(&FieldEntry(1)));
        assert_eq!(builder.schema.indexes_map.get("f2"), None);

        // add index
        builder.add_index("f2".to_string(), IndexType::Term, &vec!["f2".to_string()]);
        assert_eq!(
            builder.schema.fields[1],
            Field {
                name: "f2".to_string(),
                field_type: FieldType::Str,
                column: true,
                indexes: vec![IndexEntry(1)],
            }
        );
        assert_eq!(
            builder.schema.indexes[1],
            Index {
                name: "f2".to_string(),
                index_type: IndexType::Term,
                fields: vec![FieldEntry(1)],
            }
        );
        assert_eq!(builder.schema.indexes_map.get("f2"), Some(&IndexEntry(1)));

        // add union index
        builder.add_index(
            "f3".to_string(),
            IndexType::Term,
            &vec!["f1".to_string(), "f2".to_string()],
        );
        assert_eq!(
            builder.schema.fields[0],
            Field {
                name: "f1".to_string(),
                field_type: FieldType::Str,
                column: true,
                indexes: vec![IndexEntry(0), IndexEntry(2)],
            }
        );
        assert_eq!(
            builder.schema.fields[1],
            Field {
                name: "f2".to_string(),
                field_type: FieldType::Str,
                column: true,
                indexes: vec![IndexEntry(1), IndexEntry(2)],
            }
        );
        assert_eq!(
            builder.schema.indexes[2],
            Index {
                name: "f3".to_string(),
                index_type: IndexType::Term,
                fields: vec![FieldEntry(0), FieldEntry(1)],
            }
        );
    }
}
