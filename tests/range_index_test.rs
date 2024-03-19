use tansakuu::{
    doc,
    query::Term,
    schema::{FieldType, Schema, INDEXED},
    table::{Table, TableIndexReader, TableSettings},
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
fn test_range_index() {
    let mut schema_builder = Schema::builder();
    schema_builder.add_field("f0".to_string(), FieldType::UInt64, INDEXED);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();
    for i in 0..17_u64 {
        let doc = doc!(f0 => i);
        writer.add_document(doc);
    }

    let reader = table.reader();
    let index_reader = reader.index_reader();

    let term = Term::new("f0".to_string(), "1,3".to_string());
    let docids = get_all_docs(index_reader, &term);
    let expect = vec![1, 2, 3];
    assert_eq!(docids, expect);

    let term = Term::new("f0".to_string(), "0,16".to_string());
    let docids = get_all_docs(index_reader, &term);
    let expect: Vec<_> = (0..17).collect();
    assert_eq!(docids, expect);

    // Serialize
    writer.new_segment();

    let reader = table.reader();
    let index_reader = reader.index_reader();

    let term = Term::new("f0".to_string(), "1,3".to_string());
    let docids = get_all_docs(index_reader, &term);
    let expect = vec![1, 2, 3];
    assert_eq!(docids, expect);

    for i in 0..6_u64 {
        let doc = doc!(f0 => i);
        writer.add_document(doc);
    }

    let reader = table.reader();
    let index_reader = reader.index_reader();

    let term = Term::new("f0".to_string(), "1,3".to_string());
    let docids = get_all_docs(index_reader, &term);
    let expect = vec![1, 2, 3, 18, 19, 20];
    assert_eq!(docids, expect);

    let term = Term::new("f0".to_string(), "0,16".to_string());
    let docids = get_all_docs(index_reader, &term);
    let expect: Vec<_> = (0..23).collect();
    assert_eq!(docids, expect);

    // Merge
    writer.new_segment();

    let reader = table.reader();
    let index_reader = reader.index_reader();

    let term = Term::new("f0".to_string(), "1,3".to_string());
    let docids = get_all_docs(index_reader, &term);
    let expect = vec![1, 2, 3, 18, 19, 20];
    assert_eq!(docids, expect);

    let term = Term::new("f0".to_string(), "0,16".to_string());
    let docids = get_all_docs(index_reader, &term);
    let expect: Vec<_> = (0..23).collect();
    assert_eq!(docids, expect);
}
