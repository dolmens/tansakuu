use std::sync::Arc;

use crate::DocId;

use super::{
    table_data::SegmentDataSnapshot, Table, TableColumnReader, TableColumnReaderSnapshot,
    TableData, TableDataSnapshot, TableIndexReader, TableIndexReaderSnapshot,
};

pub struct TableReader {
    index_reader: TableIndexReader,
    column_reader: TableColumnReader,
    table_data: TableData,
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

impl<'a> TableReaderSnapshot<'a> {
    pub fn new(table: &'a Table, reader: Arc<TableReader>) -> Self {
        let mut data_snapshot = TableDataSnapshot::new();
        let mut base_docid = 0;
        for segment in reader.table_data.segments() {
            let doc_count = segment.doc_count();
            data_snapshot.segments.push(SegmentDataSnapshot {
                base_docid,
                doc_count,
            });
            base_docid += doc_count as DocId;
        }
        for segment in reader.table_data.building_segments() {
            let doc_count = segment.doc_count();
            data_snapshot.segments.push(SegmentDataSnapshot {
                base_docid,
                doc_count,
            });
            base_docid += doc_count as DocId;
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
