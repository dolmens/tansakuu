use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;

use crate::schema::SchemaRef;

use super::{TableData, TableDataRef, TableReader, TableSettings, TableSettingsRef, TableWriter};

pub struct Table {
    reader: ArcSwap<TableReader>,
    schema: SchemaRef,
    settings: TableSettingsRef,
    table_data: Mutex<TableData>,
}

pub type TableRef = Arc<Table>;

impl Table {
    pub fn open_in<P: AsRef<Path>>(
        schema: SchemaRef,
        settings: TableSettingsRef,
        _path: P,
    ) -> Self {
        let table_data = TableData::new(schema.clone(), settings.clone());
        let reader = ArcSwap::from(Arc::new(TableReader::new(table_data.clone())));
        Self {
            reader,
            schema,
            settings,
            table_data: Mutex::new(table_data),
        }
    }

    pub fn reader(&self) -> Arc<TableReader> {
        self.reader.load_full()
    }

    pub fn writer(&self) -> TableWriter {
        TableWriter::new(self)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn settings(&self) -> &TableSettingsRef {
        &self.settings
    }

    pub(crate) fn reinit_reader(&self) {
        let table_data = self.table_data().lock().unwrap();
        let reader = Arc::new(TableReader::new(table_data.clone()));
        self.reader.store(reader);
    }

    pub(crate) fn table_data(&self) -> &Mutex<TableData> {
        &self.table_data
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use crate::schema::Schema;

    use super::{Table, TableSettings};

    #[test]
    fn test_simple() {
        let schema = Arc::new(Schema::new());
        let settings = Arc::new(TableSettings::new());
        let table = Arc::new(Table::open_in(schema, settings, "."));

        // writer thread
        let table_ref = table.clone();
        let writer = thread::spawn(move || {
            let table_writer = table_ref.writer();
        });

        // reader thread
        let table_ref = table.clone();
        let reader = thread::spawn(move || loop {
            let table_reader = table_ref.reader();
            break;
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }
}
