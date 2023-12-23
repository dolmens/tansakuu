use std::sync::Arc;

use super::{
    Table, TableColumnReader, TableColumnReaderSnapshot, TableData, TableIndexReader,
    TableIndexReaderSnapshot,
};

pub struct TableReader {
    index_reader: TableIndexReader,
    column_reader: TableColumnReader,
    table_data: TableData,
}

pub struct TableDataSnapshot {
    pub segments: Vec<usize>,
}

pub struct TableReaderSnapshot<'a> {
    data_snapshot: TableDataSnapshot,
    reader: Arc<TableReader>,
    _table: &'a Table,
}

impl TableReader {
    pub fn new(table_data: TableData) -> Self {
        Self {
            index_reader: TableIndexReader::new(&table_data),
            column_reader: TableColumnReader::new(&table_data),
            table_data,
        }
    }

    pub fn index_reader(&self) -> &TableIndexReader {
        &self.index_reader
    }

    pub fn column_reader(&self) -> &TableColumnReader {
        &self.column_reader
    }
}

impl TableDataSnapshot {
    pub fn new() -> Self {
        Self { segments: vec![] }
    }
}

impl<'a> TableReaderSnapshot<'a> {
    pub fn new(table: &'a Table, reader: Arc<TableReader>) -> Self {
        let mut data_snapshot = TableDataSnapshot::new();
        let mut base_docid = 0;
        for segment in reader.table_data.segments() {
            data_snapshot.segments.push(base_docid);
            base_docid += segment.doc_count();
        }
        for segment in reader.table_data.building_segments() {
            data_snapshot.segments.push(base_docid);
            base_docid += segment.doc_count();
        }

        Self {
            data_snapshot,
            reader,
            _table: table,
        }
    }

    pub fn index_reader(&self) -> TableIndexReaderSnapshot {
        TableIndexReaderSnapshot::new(&self.data_snapshot, self.reader.index_reader())
    }

    pub fn column_reader(&self) -> TableColumnReaderSnapshot {
        TableColumnReaderSnapshot::new(&self.data_snapshot, self.reader.column_reader())
    }
}
