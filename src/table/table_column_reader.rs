use std::{collections::HashMap, ops::Deref, sync::Arc};

use crate::columnar::{ColumnReader, ColumnReaderFactory};

use super::TableData;

pub struct TableColumnReader {
    columns: HashMap<String, Arc<dyn ColumnReader>>,
}

impl TableColumnReader {
    pub fn new(table_data: &TableData) -> Self {
        let mut columns = HashMap::new();
        let column_reader_factory = ColumnReaderFactory::default();
        let schema = table_data.schema();
        for field in schema.columns() {
            let column_reader = column_reader_factory.create(field, table_data);
            columns.insert(field.name().to_string(), column_reader.into());
        }

        Self { columns }
    }

    pub fn column(&self, name: &str) -> Option<&dyn ColumnReader> {
        self.columns.get(name).map(|r| r.deref())
    }

    pub(crate) fn column_ref(&self, name: &str) -> Option<Arc<dyn ColumnReader>> {
        self.columns.get(name).map(|r| r.clone())
    }

    pub fn typed_reader<R: ColumnReader>(&self, name: &str) -> Option<&R> {
        self.columns.get(name).and_then(|r| r.downcast_ref())
    }
}
