use tansakuu::{
    columnar::{PrimitiveColumnReader, StringColumnReader},
    document::InputDocument,
    query::Term,
    schema::{FieldType, SchemaBuilder, COLUMNAR, INDEXED, PRIMARY_KEY},
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
fn test_delete_doc() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field(
        "item_id".to_string(),
        FieldType::Int64,
        COLUMNAR | INDEXED | PRIMARY_KEY,
    );
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
    let deletionmap_reader = reader.deletionmap_reader();

    assert!(!deletionmap_reader.is_deleted(0));
    assert!(!deletionmap_reader.is_deleted(1));

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

    let term_world = Term::new("title".to_string(), "world".to_string());
    assert_eq!(get_all_docs(index_reader, &term_world), vec![0, 1]);
    let term_100 = Term::new("item_id".to_string(), "100".to_string());
    writer.delete_documents(&term_100);

    assert!(deletionmap_reader.is_deleted(0));
    assert!(!deletionmap_reader.is_deleted(1));

    writer.new_segment();

    let reader = table.reader();

    let column_reader = reader.column_reader();
    let item_id_column_reader = column_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>("item_id")
        .unwrap();
    assert_eq!(item_id_column_reader.get(0), Some(200));
    let title_column_reader = column_reader
        .typed_reader::<StringColumnReader>("title")
        .unwrap();
    assert_eq!(title_column_reader.get(0), Some("world peace"));

    let index_reader = reader.index_reader();
    assert_eq!(get_all_docs(index_reader, &term_world), vec![0]);

    let mut doc3 = InputDocument::new();
    doc3.add_field("item_id".to_string(), 300 as i64);
    doc3.add_field("title".to_string(), "hello world 3");
    writer.add_document(doc3);

    assert_eq!(get_all_docs(index_reader, &term_world), vec![0, 1]);

    let mut doc4 = InputDocument::new();
    doc4.add_field("item_id".to_string(), 400 as i64);
    doc4.add_field("title".to_string(), "world peace 4");
    writer.add_document(doc4);

    assert_eq!(get_all_docs(index_reader, &term_world), vec![0, 1, 2]);

    let term_300 = Term::new("item_id".to_string(), "300".to_string());
    writer.delete_documents(&term_300);

    // trigger merge
    writer.new_segment();

    let reader = table.reader();
    let index_reader = reader.index_reader();
    assert_eq!(get_all_docs(index_reader, &term_world), vec![0, 1]);

    assert!(!deletionmap_reader.is_deleted(1));
    let term_400 = Term::new("item_id".to_string(), "400".to_string());
    // delete document in persistent segment
    writer.delete_documents(&term_400);
    let deletionmap_reader = reader.deletionmap_reader();
    assert!(deletionmap_reader.is_deleted(1));
}

#[test]
fn test_building_segment_all_deleted() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field(
        "item_id".to_string(),
        FieldType::Int64,
        COLUMNAR | INDEXED | PRIMARY_KEY,
    );
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
    let deletionmap_reader = reader.deletionmap_reader();

    assert!(!deletionmap_reader.is_deleted(0));
    assert!(!deletionmap_reader.is_deleted(1));

    let world = Term::new("title".to_string(), "world".to_string());
    assert_eq!(get_all_docs(index_reader, &world), vec![0, 1]);

    let delete_term = Term::new("item_id".to_string(), "100".to_string());
    writer.delete_documents(&delete_term);

    let delete_term = Term::new("item_id".to_string(), "200".to_string());
    writer.delete_documents(&delete_term);

    assert!(deletionmap_reader.is_deleted(0));
    assert!(deletionmap_reader.is_deleted(1));

    writer.new_segment();

    let reader = table.reader();
    let index_reader = reader.index_reader();

    let mut doc3 = InputDocument::new();
    doc3.add_field("item_id".to_string(), 300 as i64);
    doc3.add_field("title".to_string(), "hello world 3");
    writer.add_document(doc3);

    assert_eq!(get_all_docs(index_reader, &world), vec![0]);
}
