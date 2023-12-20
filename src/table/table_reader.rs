use super::{TableData, TableIndexReader, TableColumnReader};

pub struct TableReader {
    index_reader: TableIndexReader,
    column_reader: TableColumnReader,
    _table_data: TableData,
}

impl TableReader {
    pub fn new(table_data: TableData) -> Self {
        Self {
            index_reader: TableIndexReader::new(&table_data),
            column_reader: TableColumnReader::new(&table_data),
            _table_data: table_data,
        }
    }

    pub fn index_reader(&self) -> &TableIndexReader {
        &self.index_reader
    }

    pub fn column_reader(&self) -> &TableColumnReader {
        &self.column_reader
    }
}
