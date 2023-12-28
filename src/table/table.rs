use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;

use crate::schema::{Schema, SchemaRef};

use super::{
    segment::BuildingSegment, TableData, TableReader, TableReaderSnapshot, TableSettings,
    TableSettingsRef, TableWriter,
};

pub struct Table {
    reader: ArcSwap<TableReader>,
    table_data: Mutex<TableData>,
    directory: PathBuf,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableRef = Arc<Table>;

impl Table {
    pub fn open<P: AsRef<Path>>(schema: Schema, settings: TableSettings, directory: P) -> Self {
        let schema = Arc::new(schema);
        let settings = Arc::new(settings);
        let directory = directory.as_ref().to_owned();
        let table_data = TableData::new(directory.clone(), schema.clone(), settings.clone());
        let reader = ArcSwap::from(Arc::new(TableReader::new(table_data.clone())));
        Self {
            reader,
            table_data: Mutex::new(table_data),
            directory,
            schema,
            settings,
        }
    }

    pub fn reader(&self) -> TableReaderSnapshot {
        let reader = self.reader.load_full();
        TableReaderSnapshot::new(&self, reader)
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

    pub(crate) fn dump_building_segment(&self, building_segment: Arc<BuildingSegment>) {
        let mut table_data = self.table_data.lock().unwrap();
        table_data.dump_building_segment(building_segment);
        self.reinit_reader(table_data.clone());
    }

    pub fn dump_and_add_building_segment(
        &self,
        building_segment: Arc<BuildingSegment>,
        new_segment: Arc<BuildingSegment>,
    ) {
        let mut table_data = self.table_data.lock().unwrap();
        table_data.dump_and_add_building_segment(building_segment, new_segment);
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
        schema::{SchemaBuilder, COLUMN, INDEXED, PRIMARY_KEY},
        table::{primary_key_reader, Table, TableSettings},
        DocId,
    };

    fn get_all_docs(posting_iter: &mut dyn PostingIterator) -> Vec<DocId> {
        let mut docids = vec![];
        let mut docid = 0;
        loop {
            match posting_iter.seek(docid) {
                Some(seeked) => {
                    docids.push(seeked);
                    docid = seeked + 1;
                }
                None => break,
            }
        }

        docids
    }

    #[test]
    fn test_basic() {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_text_field("title".to_string(), COLUMN | INDEXED);
        let schema = schema_builder.build();
        let settings = TableSettings::new();
        let table = Table::open(schema, settings, "./testdata");

        let mut writer = table.writer();

        let mut doc1 = Document::new();
        doc1.add_field("title".to_string(), "hello world");
        writer.add_doc(&doc1);

        let mut doc2 = Document::new();
        doc2.add_field("title".to_string(), "world peace");
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
            .typed_column::<String, GenericColumnReader<_>>("title")
            .unwrap();
        assert_eq!(title_column_reader.get(0), Some("hello world".to_string()));
        assert_eq!(title_column_reader.get(1), Some("world peace".to_string()));
    }

    #[test]
    fn test_primary_key() {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_i64_field("item_id".to_string(), COLUMN | INDEXED | PRIMARY_KEY);
        schema_builder.add_text_field("title".to_string(), COLUMN | INDEXED);
        let schema = schema_builder.build();
        let settings = TableSettings::new();
        let table = Table::open(schema, settings, "./testdata");

        let mut writer = table.writer();

        let mut doc1 = Document::new();
        doc1.add_field("item_id".to_string(), 100 as i64);
        doc1.add_field("title".to_string(), "hello world");
        writer.add_doc(&doc1);

        let mut doc2 = Document::new();
        doc2.add_field("item_id".to_string(), 200 as i64);
        doc2.add_field("title".to_string(), "world peace");
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

        let term = Term::new("item_id".to_string(), "100".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![0]);

        let term = Term::new("item_id".to_string(), "200".to_string());
        let mut posting_iter = index_reader.lookup(&term).unwrap();
        let docids = get_all_docs(&mut *posting_iter);
        assert_eq!(docids, vec![1]);


        let column_reader = reader.column_reader();
        let title_column_reader = column_reader
            .typed_column::<String, GenericColumnReader<_>>("title")
            .unwrap();
        assert_eq!(title_column_reader.get(0), Some("hello world".to_string()));
        assert_eq!(title_column_reader.get(1), Some("world peace".to_string()));

        let primary_key_reader = reader.primary_key_reader().unwrap();
        let primary_key_reader = primary_key_reader.get_typed_reader::<i64>().unwrap();
        assert_eq!(primary_key_reader.get(0), Some(100));
        assert_eq!(primary_key_reader.get(1), Some(200));
    }

    #[test]
    fn test_new_segment() {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_text_field("title".to_string(), COLUMN | INDEXED);
        let schema = schema_builder.build();
        let settings = TableSettings::new();
        let table = Table::open(schema, settings, "./testdata");

        let mut writer = table.writer();

        let mut doc1 = Document::new();
        doc1.add_field("title".to_string(), "hello world");
        writer.add_doc(&doc1);

        let mut doc2 = Document::new();
        doc2.add_field("title".to_string(), "world peace");
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
            .typed_column::<String, GenericColumnReader<_>>("title")
            .unwrap();
        assert_eq!(title_column_reader.get(0), Some("hello world".to_string()));
        assert_eq!(title_column_reader.get(1), Some("world peace".to_string()));

        writer.new_segment();

        let mut doc3 = Document::new();
        doc3.add_field("title".to_string(), "hello");
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

        // writer.new_segment();
    }
}
