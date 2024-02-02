use std::{collections::HashMap, ops::BitOr, sync::Arc};

#[derive(Default)]
pub struct SchemaBuilder {
    schema: Schema,
}

#[derive(Default)]
pub struct Schema {
    fields: Vec<Field>,
    indexes: Vec<Index>,
    primary_key: Option<(FieldEntry, IndexEntry)>,
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
    multi: bool,
    column: bool,
    stored: bool,
    index_entries: Vec<IndexEntry>,
    index_names: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexType {
    InvertedIndex,
    PrimaryKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Index {
    name: String,
    index_type: IndexType,
    field_entries: Vec<FieldEntry>,
    field_names: Vec<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FieldOptions {
    multi: bool,
    column: bool,
    indexed: bool,
    stored: bool,
    primary_key: bool,
}

pub const DEFAULT: FieldOptions = FieldOptions {
    multi: false,
    column: false,
    indexed: false,
    stored: false,
    primary_key: false,
};

pub const MULTI: FieldOptions = FieldOptions {
    multi: true,
    column: false,
    indexed: false,
    stored: false,
    primary_key: false,
};

pub const COLUMN: FieldOptions = FieldOptions {
    multi: false,
    column: true,
    indexed: false,
    stored: false,
    primary_key: false,
};

pub const INDEXED: FieldOptions = FieldOptions {
    multi: false,
    column: false,
    indexed: true,
    stored: false,
    primary_key: false,
};

pub const PRIMARY_KEY: FieldOptions = FieldOptions {
    multi: false,
    column: false,
    indexed: false,
    stored: false,
    primary_key: true,
};

impl BitOr for FieldOptions {
    type Output = FieldOptions;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self {
            multi: self.multi || rhs.multi,
            column: self.column || rhs.column,
            indexed: self.indexed || rhs.indexed,
            stored: false,
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
            multi: false,
            column: options.column,
            stored: options.stored,
            index_entries: vec![],
            index_names: vec![],
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
                IndexType::InvertedIndex
            };
            self.add_index(field_name, index_type, &fields);
        }
    }

    pub fn add_index(&mut self, index_name: String, index_type: IndexType, fields: &[String]) {
        assert!(!self.schema.indexes_map.contains_key(&index_name));
        let field_names: Vec<_> = fields.iter().cloned().collect();
        let field_entries: Vec<_> = fields
            .iter()
            .map(|f| self.schema.fields_map.get(f).unwrap())
            .cloned()
            .collect();
        for entry in &field_entries {
            self.schema.fields[entry.0]
                .index_entries
                .push(IndexEntry(self.schema.indexes.len()));
            self.schema.fields[entry.0]
                .index_names
                .push(index_name.clone());
        }
        if index_type == IndexType::PrimaryKey {
            assert!(self.schema.primary_key.is_none());
            assert_eq!(fields.len(), 1);
            self.schema.primary_key =
                Some((field_entries[0], IndexEntry(self.schema.indexes.len())));
        }
        let index = Index {
            name: index_name.clone(),
            index_type,
            field_entries,
            field_names,
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

    pub fn field(&self, field_entry: FieldEntry) -> &Field {
        &self.fields[field_entry.0]
    }

    pub fn field_by_name(&self, field_name: &str) -> Option<&Field> {
        self.fields_map
            .get(field_name)
            .map(|&entry| &self.fields[entry.0])
    }

    pub fn index(&self, index_entry: IndexEntry) -> &Index {
        &self.indexes[index_entry.0]
    }

    pub fn index_by_name(&self, index_name: &str) -> Option<&Index> {
        self.indexes_map
            .get(index_name)
            .map(|&entry| &self.indexes[entry.0])
    }

    pub fn primary_key(&self) -> Option<(&Field, &Index)> {
        self.primary_key
            .map(|(field_entry, index_entry)| (self.field(field_entry), self.index(index_entry)))
    }

    pub fn indexes(&self) -> &[Index] {
        &self.indexes
    }

    pub fn columns(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter().filter(|f| f.column)
    }

    pub fn fields(&self) -> &[Field] {
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

    pub fn field_offset(&self, field: &str) -> usize {
        if self.field_names.len() == 1 {
            return 0;
        }
        for (i, f) in self.field_names.iter().enumerate() {
            if f == field {
                return i;
            }
        }
        0
    }

    pub fn fields(&self) -> &[FieldEntry] {
        &self.field_entries
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

    pub fn is_stored(&self) -> bool {
        self.stored
    }

    pub fn indexes(&self) -> &[IndexEntry] {
        &self.index_entries
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{
        schema::{FieldEntry, FieldOptions, IndexEntry, COLUMN, INDEXED},
        Field, FieldType, Index, IndexType, PRIMARY_KEY,
    };

    use super::SchemaBuilder;

    #[test]
    fn test_field_options_bitor() {
        assert_eq!(
            COLUMN | INDEXED,
            FieldOptions {
                multi: false,
                column: true,
                indexed: true,
                stored: false,
                primary_key: false,
            }
        );
    }

    #[test]
    fn test_schema_builder() {
        let mut builder = SchemaBuilder::new();
        // column and indexed
        builder.add_text_field("f1".to_string(), COLUMN | INDEXED);
        assert_eq!(
            builder.schema.fields[0],
            Field {
                name: "f1".to_string(),
                field_type: FieldType::Str,
                multi: false,
                column: true,
                stored: false,
                index_entries: vec![IndexEntry(0)],
                index_names: vec!["f1".to_string()]
            }
        );
        assert_eq!(
            builder.schema.indexes[0],
            Index {
                name: "f1".to_string(),
                index_type: IndexType::InvertedIndex,
                field_entries: vec![FieldEntry(0)],
                field_names: vec!["f1".to_string()]
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
                multi: false,
                column: true,
                stored: false,
                index_entries: vec![],
                index_names: vec![]
            }
        );
        assert_eq!(builder.schema.fields_map.get("f2"), Some(&FieldEntry(1)));
        assert_eq!(builder.schema.indexes_map.get("f2"), None);

        // add index
        builder.add_index(
            "f2".to_string(),
            IndexType::InvertedIndex,
            &vec!["f2".to_string()],
        );
        assert_eq!(
            builder.schema.fields[1],
            Field {
                name: "f2".to_string(),
                field_type: FieldType::Str,
                multi: false,
                column: true,
                stored: false,
                index_entries: vec![IndexEntry(1)],
                index_names: vec!["f2".to_string()]
            }
        );
        assert_eq!(
            builder.schema.indexes[1],
            Index {
                name: "f2".to_string(),
                index_type: IndexType::InvertedIndex,
                field_entries: vec![FieldEntry(1)],
                field_names: vec!["f2".to_string()]
            }
        );
        assert_eq!(builder.schema.indexes_map.get("f2"), Some(&IndexEntry(1)));

        // add union index
        builder.add_index(
            "f3".to_string(),
            IndexType::InvertedIndex,
            &vec!["f1".to_string(), "f2".to_string()],
        );
        assert_eq!(
            builder.schema.fields[0],
            Field {
                name: "f1".to_string(),
                field_type: FieldType::Str,
                multi: false,
                column: true,
                stored: false,
                index_entries: vec![IndexEntry(0), IndexEntry(2)],
                index_names: vec!["f1".to_string(), "f3".to_string()]
            }
        );
        assert_eq!(
            builder.schema.fields[1],
            Field {
                name: "f2".to_string(),
                field_type: FieldType::Str,
                multi: false,
                column: true,
                stored: false,
                index_entries: vec![IndexEntry(1), IndexEntry(2)],
                index_names: vec!["f2".to_string(), "f3".to_string()]
            }
        );
        assert_eq!(
            builder.schema.indexes[2],
            Index {
                name: "f3".to_string(),
                index_type: IndexType::InvertedIndex,
                field_entries: vec![FieldEntry(0), FieldEntry(1)],
                field_names: vec!["f1".to_string(), "f2".to_string()]
            }
        );
    }

    #[test]
    fn test_primary_key() {
        let mut builder = SchemaBuilder::new();
        builder.add_text_field("f1".to_string(), COLUMN | PRIMARY_KEY);
        assert_eq!(
            builder.schema.primary_key(),
            Some((&builder.schema.fields[0], &builder.schema.indexes[0]))
        );
    }

    #[test]
    fn test_columns() {
        let mut builder = SchemaBuilder::new();
        builder.add_text_field("f1".to_string(), COLUMN | INDEXED);
        builder.add_i64_field("f2".to_string(), INDEXED);
        builder.add_i64_field("f3".to_string(), COLUMN);
        let fields: Vec<_> = builder.schema.fields().iter().map(|f| f.name()).collect();
        assert_eq!(fields, vec!["f1", "f2", "f3"]);
        let columns: Vec<_> = builder.schema.columns().map(|f| f.name()).collect();
        assert_eq!(columns, vec!["f1", "f3"]);
    }
}
