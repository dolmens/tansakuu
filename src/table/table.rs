use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;

use crate::schema::{Schema, SchemaRef};

use super::{
    segment::BuildingSegment, TableData, TableReader, TableSettings, TableSettingsRef, TableWriter,
};

pub struct Table {
    reader: ArcSwap<TableReader>,
    schema: SchemaRef,
    settings: TableSettingsRef,
    table_data: Mutex<TableData>,
}

pub type TableRef = Arc<Table>;

impl Table {
    pub fn open_in<P: AsRef<Path>>(schema: Schema, settings: TableSettings, _path: P) -> Self {
        let schema = Arc::new(schema);
        let settings = Arc::new(settings);
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

    pub(crate) fn add_building_segment(&self, building_segment: BuildingSegment) {
        let mut table_data = self.table_data.lock().unwrap();
        table_data.add_building_segment(building_segment);
        self.reinit_reader(table_data.clone());
    }

    pub(crate) fn reinit_reader(&self, table_data: TableData) {
        let reader = Arc::new(TableReader::new(table_data));
        self.reader.store(reader);
    }
}

#[cfg(test)]
mod tests {}
