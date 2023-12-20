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

    pub(crate) fn add_building_segment(&self, building_segment: Arc<BuildingSegment>) {
        let mut table_data = self.table_data.lock().unwrap();
        table_data.add_building_segment(building_segment);
        self.reinit_reader(table_data.clone());
    }

    pub(crate) fn dump_segment(&self, building_segment: Arc<BuildingSegment>) {
        let mut table_data = self.table_data.lock().unwrap();
        table_data.dump_segment(building_segment);
        self.reinit_reader(table_data.clone());
    }

    pub(crate) fn reinit_reader(&self, table_data: TableData) {
        let reader = Arc::new(TableReader::new(table_data));
        self.reader.store(reader);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        column::GenericColumnReader,
        document::Document,
        index::PostingIterator,
        query::Term,
        schema::{FieldType, IndexType, Schema},
        table::{Table, TableSettings},
        DocId, END_DOCID,
    };

    fn get_all_docs(posting_iter: &mut dyn PostingIterator) -> Vec<DocId> {
        let mut docids = vec![];
        let mut docid = 0;
        loop {
            docid = posting_iter.seek(docid);
            if docid != END_DOCID {
                docids.push(docid);
                docid += 1;
            } else {
                break;
            }
        }
        docids
    }

    #[test]
    fn test_basic() {
        let mut schema = Schema::new();
        schema.add_field("title".to_string(), FieldType::Text);
        schema.add_index(
            "title".to_string(),
            IndexType::Term,
            vec!["title".to_string()],
        );
        let settings = TableSettings::new();
        let table = Table::open_in(schema, settings, ".");

        let mut writer = table.writer();

        let mut doc1 = Document::new();
        doc1.add_field("title".to_string(), "hello world".to_string());
        writer.add_doc(&doc1);

        let mut doc2 = Document::new();
        doc2.add_field("title".to_string(), "world peace".to_string());
        writer.add_doc(&doc2);

        let reader = table.reader();
        let index_reader = reader.index_reader();

        let term = Term::new("title".to_string(), "hello".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![0]);

        let term = Term::new("title".to_string(), "world".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![0, 1]);

        let term = Term::new("title".to_string(), "peace".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![1]);

        let column_reader = reader.column_reader();
        let title_column_reader = column_reader
            .column("title")
            .unwrap()
            .downcast_ref::<GenericColumnReader<String>>()
            .unwrap();
        assert_eq!(title_column_reader.get(0), Some("hello world".to_string()));
        assert_eq!(title_column_reader.get(1), Some("world peace".to_string()));
    }

    #[test]
    fn test_new_segment() {
        let mut schema = Schema::new();
        schema.add_field("title".to_string(), FieldType::Text);
        schema.add_index(
            "title".to_string(),
            IndexType::Term,
            vec!["title".to_string()],
        );
        let settings = TableSettings::new();
        let table = Table::open_in(schema, settings, ".");

        let mut writer = table.writer();

        let mut doc1 = Document::new();
        doc1.add_field("title".to_string(), "hello world".to_string());
        writer.add_doc(&doc1);

        let mut doc2 = Document::new();
        doc2.add_field("title".to_string(), "world peace".to_string());
        writer.add_doc(&doc2);

        let reader = table.reader();
        let index_reader = reader.index_reader();

        let term = Term::new("title".to_string(), "hello".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![0]);

        let term = Term::new("title".to_string(), "world".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![0, 1]);

        let term = Term::new("title".to_string(), "peace".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![1]);

        let column_reader = reader.column_reader();
        let title_column_reader = column_reader
            .column("title")
            .unwrap()
            .downcast_ref::<GenericColumnReader<String>>()
            .unwrap();
        assert_eq!(title_column_reader.get(0), Some("hello world".to_string()));
        assert_eq!(title_column_reader.get(1), Some("world peace".to_string()));

        writer.new_segment();

        let mut doc3 = Document::new();
        doc3.add_field("title".to_string(), "hello".to_string());
        writer.add_doc(&doc3);

        // Still OLD Readers
        let term = Term::new("title".to_string(), "hello".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![0]);

        let reader = table.reader();
        let index_reader = reader.index_reader();
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![0, 2]);
    }
}
