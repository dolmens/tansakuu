use std::{collections::HashMap, ops::Deref};

use crate::column::{
    ColumnReader, ColumnReaderFactory, ColumnReaderSnapshot, TypedColumnReader,
    TypedColumnReaderSnapshot,
};

use super::{TableData, TableDataSnapshot};

pub struct TableColumnReader {
    columns: HashMap<String, Box<dyn ColumnReader>>,
}

pub struct TableColumnReaderSnapshot<'a> {
    data_snapshot: &'a TableDataSnapshot,
    column_reader: &'a TableColumnReader,
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

    pub fn typed_column<T, R: TypedColumnReader<Item = T>>(&self, name: &str) -> Option<&R> {
        self.column(name).and_then(|column| column.downcast_ref())
    }
}

impl<'a> TableColumnReaderSnapshot<'a> {
    pub fn new(data_snapshot: &'a TableDataSnapshot, column_reader: &'a TableColumnReader) -> Self {
        Self {
            data_snapshot,
            column_reader,
        }
    }

    pub fn column(&self, name: &str) -> Option<ColumnReaderSnapshot> {
        self.column_reader
            .column(name)
            .map(|column| ColumnReaderSnapshot::new(self.data_snapshot, column))
    }

    pub fn typed_column<T, R: TypedColumnReader<Item = T>>(
        &self,
        name: &str,
    ) -> Option<TypedColumnReaderSnapshot<'_, T, R>> {
        self.column_reader
            .typed_column::<T, R>(name)
            .map(|column| TypedColumnReaderSnapshot::new(self.data_snapshot, column))
    }
}
