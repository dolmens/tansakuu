use tansakuu::{
    columnar::{PrimitiveColumnReader, StringColumnReader},
    document::InputDocument,
    query::Term,
    schema::{DataType, SchemaBuilder, COLUMNAR, INDEXED, PRIMARY_KEY},
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
fn test_primary_key() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_field(
        "item_id".to_string(),
        DataType::Int64,
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
    let title_column_reader = column_reader
        .typed_reader::<StringColumnReader>("title")
        .unwrap();
    assert_eq!(title_column_reader.get(0), Some("hello world"));
    assert_eq!(title_column_reader.get(1), Some("world peace"));

    let primary_key_reader = reader.primary_key_reader().unwrap();
    let primary_key_reader = primary_key_reader
        .typed_reader::<PrimitiveColumnReader<Int64Type>>()
        .unwrap();
    assert_eq!(primary_key_reader.get(0), Some(100));
    assert_eq!(primary_key_reader.get(1), Some(200));
}
