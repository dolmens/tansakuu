use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use uuid::Uuid;

use crate::{
    column::ColumnSerializerFactory,
    index::IndexSerializerFactory,
    schema::{Schema, SchemaRef},
};

use super::{
    segment::{BuildingSegment, SegmentDumper},
    TableData, TableReader, TableSettings, TableSettingsRef, TableWriter,
};

pub struct Table {
    reader: ArcSwap<TableReader>,
    segment_dumper: SegmentDumper,
    table_data: Mutex<TableData>,
    directory: PathBuf,
    schema: SchemaRef,
    settings: TableSettingsRef,
}

pub type TableRef = Arc<Table>;

impl Table {
    pub fn open_in<P: AsRef<Path>>(schema: Schema, settings: TableSettings, direcoty: P) -> Self {
        let schema = Arc::new(schema);
        let settings = Arc::new(settings);
        let table_data = TableData::new(schema.clone(), settings.clone());
        let reader = ArcSwap::from(Arc::new(TableReader::new(table_data.clone())));
        Self {
            reader,
            segment_dumper: SegmentDumper::new(),
            table_data: Mutex::new(table_data),
            directory: direcoty.as_ref().into(),
            schema,
            settings,
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
        let segment_uuid = Uuid::new_v4();
        let segment_uuid_string = segment_uuid.as_simple().to_string();
        let segment_directory = self.directory.join("segments");
        let dumping_segment_directory = segment_directory.join(segment_uuid_string);

        let column_directory = dumping_segment_directory.join("column");
        fs::create_dir_all(&column_directory).unwrap();
        let column_serializer_factory = ColumnSerializerFactory::new();
        for field in self.schema.columns() {
            let column_data = building_segment
                .column_data()
                .column_data(field.name())
                .unwrap()
                .clone();
            let column_serializer = column_serializer_factory.create(field, column_data);
            column_serializer.serialize(&column_directory);
        }

        let index_directory = dumping_segment_directory.join("index");
        fs::create_dir_all(&index_directory).unwrap();
        let index_serializer_factory = IndexSerializerFactory::new();
        for index in self.schema.indexes() {
            let index_data = building_segment
                .index_data()
                .index_data(index.name())
                .unwrap()
                .clone();
            let index_serializer = index_serializer_factory.create(index, index_data);
            index_serializer.serialize(&index_directory);
        }
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
        let table = Table::open_in(schema, settings, "./testdata");

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
        let table = Table::open_in(schema, settings, "./testdata");

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
