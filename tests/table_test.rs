use tansakuu::{
    columnar::{PrimitiveColumnReader, StringColumnReader},
    document::InputDocument,
    query::Term,
    schema::{SchemaBuilder, COLUMNAR, INDEXED, PRIMARY_KEY},
    table::{Table, TableIndexReader, TableSettings},
    types::Int64Type,
    DocId, END_DOCID,
};

fn get_all_docs(index_reader: &TableIndexReader, term: &Term) -> Vec<DocId> {
    let mut docids = vec![];
    let mut posting_iter = index_reader.lookup(&term);
    if let Some(posting_iter) = posting_iter.as_deref_mut() {
        let mut docid = 0;
        loop {
            docid = posting_iter.seek(docid).unwrap();
            if docid != END_DOCID {
                docids.push(docid);
                docid += 1;
            } else {
                break;
            }
        }
    }
    docids
}

#[test]
fn test_basic() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_text_field("title".to_string(), COLUMNAR | INDEXED);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let mut doc1 = InputDocument::new();
    doc1.add_field("title".to_string(), "hello world");
    writer.add_document(doc1);

    let mut doc2 = InputDocument::new();
    doc2.add_field("title".to_string(), "world peace");
    writer.add_document(doc2);

    let reader = table.reader();
    let index_reader = reader.index_reader();

    let term = Term::new("title".to_string(), "hello".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    let term = Term::new("title".to_string(), "world".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 1]);

    let term = Term::new("title".to_string(), "peace".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![1]);

    let column_reader = reader.column_reader();
    let title_column_reader = column_reader
        .typed_reader::<StringColumnReader>("title")
        .unwrap();
    assert_eq!(title_column_reader.get(0), Some("hello world"));
    assert_eq!(title_column_reader.get(1), Some("world peace"));
}

#[test]
fn test_segment_serialize() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_i64_field("item_id".to_string(), COLUMNAR | INDEXED | PRIMARY_KEY);
    schema_builder.add_text_field("title".to_string(), COLUMNAR | INDEXED);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let mut doc1 = InputDocument::new();
    doc1.add_field("item_id".to_string(), 100 as i64);
    doc1.add_field("title".to_string(), "hello world");
    writer.add_document(doc1);

    let mut doc2 = InputDocument::new();
    doc2.add_field("item_id".to_string(), 200 as i64);
    doc2.add_field("title".to_string(), "world peace");
    writer.add_document(doc2);

    let reader = table.reader();
    let index_reader = reader.index_reader();

    let term = Term::new("title".to_string(), "hello".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    let term = Term::new("title".to_string(), "world".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 1]);

    let term = Term::new("title".to_string(), "peace".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![1]);

    let term = Term::new("item_id".to_string(), "100".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    let term = Term::new("item_id".to_string(), "200".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![1]);

    let column_reader = reader.column_reader();
    let item_id_column_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("item_id")
        .unwrap();
    assert_eq!(item_id_column_reader.get(0), Some(100));
    assert_eq!(item_id_column_reader.get(1), Some(200));
    let title_column_reader = column_reader
        .typed_reader::<StringColumnReader>("title")
        .unwrap();
    assert_eq!(title_column_reader.get(0), Some("hello world"));
    assert_eq!(title_column_reader.get(1), Some("world peace"));

    writer.new_segment();

    let mut doc3 = InputDocument::new();
    doc3.add_field("item_id".to_string(), 300 as i64);
    doc3.add_field("title".to_string(), "hello");
    writer.add_document(doc3);

    // Still OLD Readers
    let term = Term::new("title".to_string(), "hello".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    // Open new reader
    let reader = table.reader();

    let index_reader = reader.index_reader();
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 2]);

    let term = Term::new("title".to_string(), "world".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 1]);

    let term = Term::new("item_id".to_string(), "100".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    let term = Term::new("item_id".to_string(), "200".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![1]);

    let term = Term::new("item_id".to_string(), "300".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![2]);

    let column_reader = reader.column_reader();
    let item_id_column_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("item_id")
        .unwrap();
    assert_eq!(item_id_column_reader.get(0), Some(100));
    assert_eq!(item_id_column_reader.get(1), Some(200));
    assert_eq!(item_id_column_reader.get(2), Some(300));
    let title_column_reader = column_reader
        .typed_reader::<StringColumnReader>("title")
        .unwrap();
    assert_eq!(title_column_reader.get(0), Some("hello world"));
    assert_eq!(title_column_reader.get(1), Some("world peace"));
    assert_eq!(title_column_reader.get(2), Some("hello"));
}

#[test]
fn test_segment_merge() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_i64_field("item_id".to_string(), COLUMNAR | INDEXED | PRIMARY_KEY);
    schema_builder.add_text_field("title".to_string(), COLUMNAR | INDEXED);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    let mut doc1 = InputDocument::new();
    doc1.add_field("item_id".to_string(), 100 as i64);
    doc1.add_field("title".to_string(), "hello world");
    writer.add_document(doc1);

    let mut doc2 = InputDocument::new();
    doc2.add_field("item_id".to_string(), 200 as i64);
    doc2.add_field("title".to_string(), "world peace");
    writer.add_document(doc2);

    let reader = table.reader();
    let index_reader = reader.index_reader();

    let term = Term::new("title".to_string(), "hello".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    let term = Term::new("title".to_string(), "world".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 1]);

    let term = Term::new("title".to_string(), "peace".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![1]);

    let term = Term::new("item_id".to_string(), "100".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    let term = Term::new("item_id".to_string(), "200".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![1]);

    let column_reader = reader.column_reader();
    let item_id_column_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("item_id")
        .unwrap();
    assert_eq!(item_id_column_reader.get(0), Some(100));
    assert_eq!(item_id_column_reader.get(1), Some(200));
    let title_column_reader = column_reader
        .typed_reader::<StringColumnReader>("title")
        .unwrap();
    assert_eq!(title_column_reader.get(0), Some("hello world"));
    assert_eq!(title_column_reader.get(1), Some("world peace"));

    writer.new_segment();

    let mut doc3 = InputDocument::new();
    doc3.add_field("item_id".to_string(), 300 as i64);
    doc3.add_field("title".to_string(), "hello");
    writer.add_document(doc3);

    // Still OLD Readers
    let term = Term::new("title".to_string(), "hello".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    // Open new reader
    let reader = table.reader();

    let index_reader = reader.index_reader();
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 2]);

    let term = Term::new("title".to_string(), "world".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 1]);

    let term = Term::new("item_id".to_string(), "100".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    let term = Term::new("item_id".to_string(), "200".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![1]);

    let term = Term::new("item_id".to_string(), "300".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![2]);

    let column_reader = reader.column_reader();
    let item_id_column_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("item_id")
        .unwrap();
    assert_eq!(item_id_column_reader.get(0), Some(100));
    assert_eq!(item_id_column_reader.get(1), Some(200));
    assert_eq!(item_id_column_reader.get(2), Some(300));
    let title_column_reader = column_reader
        .typed_reader::<StringColumnReader>("title")
        .unwrap();
    assert_eq!(title_column_reader.get(0), Some("hello world"));
    assert_eq!(title_column_reader.get(1), Some("world peace"));
    assert_eq!(title_column_reader.get(2), Some("hello"));

    writer.new_segment();

    let term = Term::new("title".to_string(), "hello".to_string());

    // Open new reader
    let reader = table.reader();

    let index_reader = reader.index_reader();
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 2]);

    let term = Term::new("title".to_string(), "world".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0, 1]);

    let term = Term::new("item_id".to_string(), "100".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![0]);

    let term = Term::new("item_id".to_string(), "200".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![1]);

    let term = Term::new("item_id".to_string(), "300".to_string());
    let docids = get_all_docs(index_reader, &term);
    assert_eq!(docids, vec![2]);

    let column_reader = reader.column_reader();
    let item_id_column_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("item_id")
        .unwrap();
    assert_eq!(item_id_column_reader.get(0), Some(100));
    assert_eq!(item_id_column_reader.get(1), Some(200));
    assert_eq!(item_id_column_reader.get(2), Some(300));
    let title_column_reader = column_reader
        .typed_reader::<StringColumnReader>("title")
        .unwrap();
    assert_eq!(title_column_reader.get(0), Some("hello world"));
    assert_eq!(title_column_reader.get(1), Some("world peace"));
    assert_eq!(title_column_reader.get(2), Some("hello"));
}
