use std::sync::Arc;

use crate::{deletionmap::DeletionMapReader, index, DocId};

use super::{segment::SegmentId, PrimaryKeyReader, TableColumnReader, TableData, TableIndexReader};

pub struct TableReader {
    index_reader: TableIndexReader,
    column_reader: TableColumnReader,
    primary_key_reader: Option<PrimaryKeyReader>,
    primary_key_index_reader: Option<Arc<index::UniqueKeyReader>>,
    deletionmap_reader: DeletionMapReader,
    table_data: TableData,
}

impl TableReader {
    pub fn new(table_data: TableData) -> Self {
        let index_reader = TableIndexReader::new(&table_data);
        let column_reader = TableColumnReader::new(&table_data);
        let primary_key_reader = table_data
            .schema()
            .primary_key()
            .and_then(|(primary_key, _)| {
                column_reader
                    .column_ref(primary_key.name())
                    .map(|r| PrimaryKeyReader::new(r))
            });
        let primary_key_index_reader = table_data
            .schema()
            .primary_key()
            .and_then(|(_, primary_key)| index_reader.index_ref(primary_key.name()))
            .and_then(|reader| reader.downcast_arc().ok());

        let deletionmap_reader = DeletionMapReader::new(&table_data);

        Self {
            index_reader,
            column_reader,
            primary_key_reader,
            primary_key_index_reader,
            deletionmap_reader,
            table_data,
        }
    }

    pub fn index_reader(&self) -> &TableIndexReader {
        &self.index_reader
    }

    pub fn column_reader(&self) -> &TableColumnReader {
        &self.column_reader
    }

    pub fn primary_key_reader(&self) -> Option<&PrimaryKeyReader> {
        self.primary_key_reader.as_ref()
    }

    pub fn primary_key_index_reader(&self) -> Option<&index::UniqueKeyReader> {
        self.primary_key_index_reader.as_deref()
    }

    pub fn deletionmap_reader(&self) -> &DeletionMapReader {
        &self.deletionmap_reader
    }

    pub fn segment_of_docid(&self, docid: DocId) -> Option<(&SegmentId, DocId)> {
        self.table_data.segment_of_docid(docid)
    }
}
