use std::sync::Arc;

use crate::DocId;

use super::{
    table_data::SegmentDataSnapshot, PrimaryKeyReaderSnapshot, Table, TableColumnReader,
    TableColumnReaderSnapshot, TableData, TableDataSnapshot, TableIndexReader,
    TableIndexReaderSnapshot,
};

pub struct TableReader {
    index_reader: TableIndexReader,
    column_reader: TableColumnReader,
    table_data: TableData,
}

pub struct TableReaderSnapshot<'a> {
    reader: Arc<TableReader>,
    snapshot: TableDataSnapshot,
    _table: &'a Table,
}

impl TableReader {
    pub fn new(table_data: TableData) -> Self {
        let index_reader = TableIndexReader::new(&table_data);
        let column_reader = TableColumnReader::new(&table_data);

        Self {
            index_reader,
            column_reader,
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
            reader,
            snapshot: data_snapshot,
            _table: table,
        }
    }

    pub fn index_reader(&self) -> TableIndexReaderSnapshot {
        TableIndexReaderSnapshot::new(self.reader.index_reader(), &self.snapshot)
    }

    pub fn column_reader(&self) -> TableColumnReaderSnapshot {
        TableColumnReaderSnapshot::new(&self.snapshot, self.reader.column_reader())
    }

    pub fn primary_key_reader(&self) -> Option<PrimaryKeyReaderSnapshot> {
        self.reader
            .table_data
            .schema()
            .primary_key()
            .and_then(|(pk, _)| self.reader.column_reader.column(&pk))
            .map(|reader| PrimaryKeyReaderSnapshot::new(reader, &self.snapshot))
    }
}
