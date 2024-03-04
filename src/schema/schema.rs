use std::{collections::HashMap, ops::BitOr, sync::Arc};

use super::DataType;

#[derive(Default)]
pub struct SchemaBuilder {
    schema: Schema,
}

#[derive(Default)]
pub struct Schema {
    fields: Vec<FieldRef>,
    columns: Vec<FieldRef>,
    indexes: Vec<IndexRef>,
    primary_key: Option<(FieldRef, IndexRef)>,
    fields_map: HashMap<String, (FieldRef, Vec<IndexRef>)>,
    indexes_map: HashMap<String, IndexRef>,
}

pub type SchemaRef = Arc<Schema>;

#[derive(Debug)]
pub struct Field {
    name: String,
    field_type: DataType,
    multi: bool,
    columnar: bool,
    stored: bool,
}

pub type FieldRef = Arc<Field>;

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct TextIndexOptions {
    pub has_tflist: bool,
    pub has_fieldmask: bool,
    pub has_position_list: bool,
    pub tokenizer: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexType {
    Text(TextIndexOptions),
    PrimaryKey,
    UniqueKey,
}

#[derive(Debug)]
pub struct Index {
    name: String,
    index_type: IndexType,
    fields: Vec<FieldRef>,
}

pub type IndexRef = Arc<Index>;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FieldOptions {
    multi: bool,
    columnar: bool,
    indexed: bool,
    stored: bool,
    unique_key: bool,
    primary_key: bool,
}

pub const BARE_FIELD: FieldOptions = FieldOptions {
    multi: false,
    columnar: false,
    indexed: false,
    stored: false,
    unique_key: false,
    primary_key: false,
};

pub const MULTI: FieldOptions = FieldOptions {
    multi: true,
    columnar: false,
    indexed: false,
    stored: false,
    unique_key: false,
    primary_key: false,
};

pub const COLUMNAR: FieldOptions = FieldOptions {
    multi: false,
    columnar: true,
    indexed: false,
    stored: false,
    unique_key: false,
    primary_key: false,
};

pub const INDEXED: FieldOptions = FieldOptions {
    multi: false,
    columnar: false,
    indexed: true,
    stored: false,
    unique_key: false,
    primary_key: false,
};

pub const UNIQUE_KEY: FieldOptions = FieldOptions {
    multi: false,
    columnar: false,
    indexed: true,
    stored: false,
    unique_key: true,
    primary_key: false,
};

pub const PRIMARY_KEY: FieldOptions = FieldOptions {
    multi: false,
    columnar: false,
    indexed: true,
    stored: false,
    unique_key: false,
    primary_key: true,
};

impl BitOr for FieldOptions {
    type Output = FieldOptions;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self {
            multi: self.multi || rhs.multi,
            columnar: self.columnar || rhs.columnar,
            indexed: self.indexed || rhs.indexed,
            stored: false,
            unique_key: self.unique_key || rhs.unique_key,
            primary_key: self.primary_key || rhs.primary_key,
        }
    }
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_text_field(&mut self, field_name: String, options: FieldOptions) {
        self.add_field(field_name, DataType::String, options);
    }

    pub fn add_i64_field(&mut self, field_name: String, options: FieldOptions) {
        self.add_field(field_name, DataType::Int64, options);
    }

    pub fn add_field(&mut self, field_name: String, field_type: DataType, options: FieldOptions) {
        assert!(
            !self.schema.fields_map.contains_key(&field_name),
            "Field `{field_name}` already exist."
        );

        let mut options = options;
        if options.primary_key {
            options.columnar = true;
            options.indexed = true;
        }

        let field = Arc::new(Field {
            name: field_name,
            field_type,
            multi: options.multi,
            columnar: options.columnar,
            stored: options.stored,
        });
        if field.columnar {
            self.schema.columns.push(field.clone());
        }
        self.schema
            .fields_map
            .insert(field.name().to_string(), (field.clone(), Vec::new()));

        if options.indexed {
            let fields = vec![field.name().to_string()];
            let index_type = if options.primary_key {
                IndexType::PrimaryKey
            } else if options.unique_key {
                IndexType::UniqueKey
            } else {
                IndexType::Text(Default::default())
            };
            self.add_index(field.name().to_string(), index_type, &fields);
        }

        self.schema.fields.push(field);
    }

    pub fn add_index(&mut self, index_name: String, index_type: IndexType, fields: &[String]) {
        assert!(
            !self.schema.indexes_map.contains_key(&index_name),
            "Index `{index_name}` alreay exist."
        );

        let field_refs: Vec<_> = fields
            .iter()
            .map(|f| self.schema.fields_map.get(f).unwrap().0.clone())
            .collect();

        if matches!(index_type, IndexType::PrimaryKey) {
            // TODO: validate field type, only primitive allowed
            assert_eq!(
                field_refs.len(),
                1,
                "PrimaryKey `{index_name}` should only index one field."
            );
            assert!(
                !field_refs[0].multi,
                "PrimaryKey `{index_name}` field should not be multi."
            )
        }

        if matches!(index_type, IndexType::UniqueKey) {
            // TODO: validate field type, only primitive allowed
            assert_eq!(
                field_refs.len(),
                1,
                "UniqueKey `{index_name}` should only index one field."
            );
            assert!(
                !field_refs[0].multi,
                "UniqueKey `{index_name}` field should not be multi."
            )
        }

        let index = Arc::new(Index {
            name: index_name,
            index_type,
            fields: field_refs,
        });

        for field in fields {
            self.schema
                .fields_map
                .get_mut(field)
                .unwrap()
                .1
                .push(index.clone());
        }

        if matches!(index.index_type(), IndexType::PrimaryKey) {
            assert!(
                self.schema.primary_key.is_none(),
                "PrimaryKey already exist.",
            );
            self.schema.primary_key = Some((index.fields[0].clone(), index.clone()));
        }

        self.schema
            .indexes_map
            .insert(index.name().to_string(), index.clone());
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

    pub fn field(&self, field_name: &str) -> Option<(&FieldRef, &[IndexRef])> {
        self.fields_map
            .get(field_name)
            .map(|(field, indexes)| (field, indexes.as_slice()))
    }

    pub fn index(&self, index_name: &str) -> Option<&IndexRef> {
        self.indexes_map.get(index_name)
    }

    pub fn primary_key(&self) -> Option<(&FieldRef, &IndexRef)> {
        self.primary_key
            .as_ref()
            .map(|(field, index)| (field, index))
    }

    pub fn indexes(&self) -> &[IndexRef] {
        &self.indexes
    }

    pub fn columns(&self) -> &[FieldRef] {
        &self.columns
    }

    pub fn fields(&self) -> &[FieldRef] {
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

    pub fn field_offset(&self, field: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name() == field)
    }

    pub fn fields(&self) -> &[FieldRef] {
        &self.fields
    }
}

impl Field {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.field_type
    }

    pub fn is_column(&self) -> bool {
        self.columnar
    }

    pub fn is_stored(&self) -> bool {
        self.stored
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::schema::{
        schema::FieldOptions, IndexType, SchemaBuilder, COLUMNAR, INDEXED, PRIMARY_KEY,
    };

    #[test]
    fn test_field_options_bitor() {
        assert_eq!(
            COLUMNAR | INDEXED,
            FieldOptions {
                multi: false,
                columnar: true,
                indexed: true,
                stored: false,
                unique_key: false,
                primary_key: false,
            }
        );
    }

    #[test]
    fn test_schema_builder() {
        let mut builder = SchemaBuilder::new();
        {
            // column and indexed
            builder.add_text_field("f1".to_string(), COLUMNAR | INDEXED);
            assert_eq!(builder.schema.fields.len(), 1);
            let field1 = &builder.schema.fields[0];
            assert_eq!(field1.name(), "f1");
            assert_eq!(field1.columnar, true);
            assert_eq!(builder.schema.indexes.len(), 1);
            let index1 = &builder.schema.indexes[0];
            assert_eq!(index1.name(), "f1");
            assert_eq!(builder.schema.columns.len(), 1);
            let column1 = &builder.schema.columns[0];
            assert!(Arc::ptr_eq(field1, column1));
            assert_eq!(index1.fields.len(), 1);
            assert!(Arc::ptr_eq(field1, &index1.fields[0]));
            assert_eq!(builder.schema.fields_map.len(), 1);
            let (field, indexes) = builder.schema.fields_map.get("f1").unwrap();
            assert!(Arc::ptr_eq(field1, field));
            assert_eq!(indexes.len(), 1);
            assert!(Arc::ptr_eq(index1, &indexes[0]));
            assert_eq!(builder.schema.indexes_map.len(), 1);
            let index = builder.schema.indexes_map.get("f1").unwrap();
            assert!(Arc::ptr_eq(index1, index));
        }

        {
            // only column
            builder.add_text_field("f2".to_string(), COLUMNAR);
            assert_eq!(builder.schema.fields.len(), 2);
            let field2 = &builder.schema.fields[1];
            assert_eq!(field2.name(), "f2");
            assert_eq!(builder.schema.columns.len(), 2);
            assert!(Arc::ptr_eq(field2, &builder.schema.columns[1]));
            assert_eq!(builder.schema.indexes.len(), 1);
            assert_eq!(builder.schema.fields_map.len(), 2);
            let (field, indexes) = builder.schema.fields_map.get("f2").unwrap();
            assert!(Arc::ptr_eq(field2, field));
            assert_eq!(indexes.len(), 0);
        }

        {
            // add index
            builder.add_index(
                "i2".to_string(),
                IndexType::Text(Default::default()),
                &vec!["f2".to_string()],
            );
            assert_eq!(builder.schema.indexes.len(), 2);
            let index2 = &builder.schema.indexes[1];
            assert_eq!(index2.name(), "i2");
            let index = builder.schema.indexes_map.get("i2").unwrap();
            assert!(Arc::ptr_eq(index2, index));
            let (field, indexes) = builder.schema.fields_map.get("f2").unwrap();
            let field2 = &builder.schema.fields[1];
            assert!(Arc::ptr_eq(field2, field));
            assert_eq!(indexes.len(), 1);
        }

        {
            // add union index
            builder.add_index(
                "i3".to_string(),
                IndexType::Text(Default::default()),
                &vec!["f1".to_string(), "f2".to_string()],
            );
            assert_eq!(builder.schema.indexes.len(), 3);
            let index3 = &builder.schema.indexes[2];
            assert_eq!(index3.name(), "i3");
            assert_eq!(builder.schema.indexes_map.len(), 3);
            let index = builder.schema.indexes_map.get("i3").unwrap();
            assert!(Arc::ptr_eq(index3, index));
            assert_eq!(index3.fields.len(), 2);
            assert_eq!(index3.fields[0].name(), "f1");
            assert_eq!(index3.fields[1].name(), "f2");
            let (field1, indexes1) = builder.schema.fields_map.get("f1").unwrap();
            assert!(Arc::ptr_eq(field1, &builder.schema.fields[0]));
            assert_eq!(indexes1.len(), 2);
            assert!(Arc::ptr_eq(&indexes1[0], &builder.schema.indexes[0]));
            assert!(Arc::ptr_eq(&indexes1[1], &builder.schema.indexes[2]));
            let (field2, indexes2) = builder.schema.fields_map.get("f2").unwrap();
            assert!(Arc::ptr_eq(field2, &builder.schema.fields[1]));
            assert_eq!(indexes1.len(), 2);
            assert!(Arc::ptr_eq(&indexes2[0], &builder.schema.indexes[1]));
            assert!(Arc::ptr_eq(&indexes2[1], &builder.schema.indexes[2]));
        }
    }

    #[test]
    fn test_primary_key() {
        let mut builder = SchemaBuilder::default();
        builder.add_text_field("f1".to_string(), COLUMNAR | PRIMARY_KEY);
        let (field, indexes) = builder.schema.field("f1").unwrap();
        let (pk_field, pk_index) = builder.schema.primary_key().unwrap();
        assert!(Arc::ptr_eq(field, pk_field));
        assert_eq!(indexes.len(), 1);
        assert!(Arc::ptr_eq(&indexes[0], pk_index));
    }

    #[test]
    fn test_columns() {
        let mut builder = SchemaBuilder::default();
        builder.add_text_field("f1".to_string(), COLUMNAR | INDEXED);
        builder.add_i64_field("f2".to_string(), INDEXED);
        builder.add_i64_field("f3".to_string(), COLUMNAR);
        let fields: Vec<_> = builder.schema.fields().iter().map(|f| f.name()).collect();
        assert_eq!(fields, vec!["f1", "f2", "f3"]);
        let columns: Vec<_> = builder.schema.columns().iter().map(|f| f.name()).collect();
        assert_eq!(columns, vec!["f1", "f3"]);
    }
}
