use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use crate::{
    directory::RamDirectory,
    schema::{Schema, SchemaRef},
    Directory,
};

use super::{SegmentStat, TableData, TableReader, TableSettings, TableSettingsRef, TableWriter};

#[derive(Clone)]
pub struct Table {
    inner: Arc<TableInner>,
}

struct TableInner {
    reader: ArcSwap<TableReader>,
    table_data: Mutex<TableData>,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

impl Table {
    pub fn create(schema: Schema, settings: TableSettings) -> Self {
        Self::open(RamDirectory::create(), schema, settings)
    }

    pub fn open<D: Into<Box<dyn Directory>>>(
        directory: D,
        schema: Schema,
        settings: TableSettings,
    ) -> Self {
        let schema = Arc::new(schema);
        let settings = Arc::new(settings);
        let table_data = TableData::open(directory, schema.clone(), settings.clone());
        let reader = ArcSwap::from(Arc::new(TableReader::new(table_data.clone())));

        Self {
            inner: Arc::new(TableInner {
                reader,
                table_data: Mutex::new(table_data),
                schema,
                settings,
            }),
        }
    }

    pub fn reader(&self) -> Arc<TableReader> {
        self.inner.reader.load_full()
    }

    pub fn writer(&self) -> TableWriter {
        TableWriter::new(self)
    }

    pub(crate) fn data(&self) -> &Mutex<TableData> {
        &self.inner.table_data
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.inner.schema
    }

    pub fn settings(&self) -> &TableSettingsRef {
        &self.inner.settings
    }

    pub fn recent_segment_stat(&self) -> Option<Arc<SegmentStat>> {
        let table_data = self.inner.table_data.lock().unwrap();
        table_data.recent_segment_stat().map(|stat| stat.clone())
    }

    pub(crate) fn reinit_reader(&self, table_data: TableData) {
        let reader = Arc::new(TableReader::new(table_data));
        self.inner.reader.store(reader);
    }
}
