use std::{collections::HashMap, ops::Deref};

use crate::column::{ColumnReader, ColumnReaderFactory};

use super::TableData;

pub struct TableColumnReader {
    columns: HashMap<String, Box<dyn ColumnReader>>,
}

impl TableColumnReader {
    pub fn new(table_data: &TableData) -> Self {
        let mut columns = HashMap::new();
        let column_reader_factory = ColumnReaderFactory::new();
        let schema = table_data.schema();
        for field in schema.columns() {
            let column_reader = column_reader_factory.create(field, table_data);
            columns.insert(field.name().to_string(), column_reader);
        }

        Self { columns }
    }

    pub fn column(&self, name: &str) -> Option<&dyn ColumnReader> {
        self.columns.get(name).map(|r| r.deref())
    }
}
