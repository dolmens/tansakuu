use std::sync::Arc;

use super::{TableData, TableIndexReader};

pub struct TableReader {
    index_reader: TableIndexReader,
    table_data: TableData,
}

impl TableReader {
    pub fn new(table_data: TableData) -> Self {
        Self {
            index_reader: TableIndexReader::new(&table_data),
            table_data,
        }
    }

    pub fn index_reader(&self) -> &TableIndexReader {
        &self.index_reader
    }
}
