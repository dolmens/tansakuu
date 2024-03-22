use tansakuu::{
    columnar::BooleanColumnReader,
    doc,
    query::Term,
    schema::{FieldType, Schema, COLUMNAR, INDEXED, NOT_NULL},
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
fn test_boolean_column() {
    let mut schema_builder = Schema::builder();
    schema_builder.add_field("f0".to_string(), FieldType::Boolean, INDEXED | COLUMNAR);
    schema_builder.add_field(
        "f1".to_string(),
        FieldType::Boolean,
        INDEXED | COLUMNAR | NOT_NULL,
    );
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    writer.add_document(doc!(f0 => true, f1 => false));
    writer.add_document(doc!(f0 => false, f1 => true));
    writer.add_document(doc!());

    let reader = table.reader();

    let f0_reader = reader
        .column_reader()
        .column("f0")
        .unwrap()
        .downcast_ref::<BooleanColumnReader>()
        .unwrap();
    let f1_reader = reader
        .column_reader()
        .column("f1")
        .unwrap()
        .downcast_ref::<BooleanColumnReader>()
        .unwrap();

    assert_eq!(f0_reader.get(0), Some(true));
    assert_eq!(f1_reader.get(0), Some(false));
    assert_eq!(f0_reader.get(1), Some(false));
    assert_eq!(f1_reader.get(1), Some(true));
    assert_eq!(f0_reader.get(2), None);
    assert_eq!(f1_reader.get(2), Some(false));

    // let index_reader = reader.index_reader();
    // let term_f0_true = Term::new("f0".to_string(), "true".to_string());
    // let term_f0_false = Term::new("f0".to_string(), "false".to_string());
    // let term_f1_true = Term::new("f1".to_string(), "true".to_string());
    // let term_f1_false = Term::new("f1".to_string(), "false".to_string());

    // assert_eq!(get_all_docs(index_reader, &term_f0_true), vec![0]);
    // assert_eq!(get_all_docs(index_reader, &term_f0_false), vec![1]);

    // // Serialize
    // writer.new_segment();

    // writer.add_document(doc!(f0 => false, f1 => true));

    // let reader = table.reader();

    // let f0_reader = reader
    //     .column_reader()
    //     .column("f0")
    //     .unwrap()
    //     .downcast_ref::<BooleanColumnReader>()
    //     .unwrap();
    // let f1_reader = reader
    //     .column_reader()
    //     .column("f1")
    //     .unwrap()
    //     .downcast_ref::<BooleanColumnReader>()
    //     .unwrap();

    // assert_eq!(f0_reader.get(0), Some(true));
    // assert_eq!(f1_reader.get(0), Some(false));
    // assert_eq!(f0_reader.get(1), Some(false));
    // assert_eq!(f1_reader.get(1), Some(true));
    // assert_eq!(f0_reader.get(2), None);
    // assert_eq!(f1_reader.get(2), Some(false));
    // assert_eq!(f0_reader.get(3), Some(false));
    // assert_eq!(f1_reader.get(3), Some(true));

    // // Merge
    // writer.new_segment();

    // writer.add_document(doc!(f0 => true, f1 => false));

    // let reader = table.reader();

    // let f0_reader = reader
    //     .column_reader()
    //     .column("f0")
    //     .unwrap()
    //     .downcast_ref::<BooleanColumnReader>()
    //     .unwrap();
    // let f1_reader = reader
    //     .column_reader()
    //     .column("f1")
    //     .unwrap()
    //     .downcast_ref::<BooleanColumnReader>()
    //     .unwrap();

    // assert_eq!(f0_reader.get(0), Some(true));
    // assert_eq!(f1_reader.get(0), Some(false));
    // assert_eq!(f0_reader.get(1), Some(false));
    // assert_eq!(f1_reader.get(1), Some(true));
    // assert_eq!(f0_reader.get(2), None);
    // assert_eq!(f1_reader.get(2), Some(false));
    // assert_eq!(f0_reader.get(3), Some(false));
    // assert_eq!(f1_reader.get(3), Some(true));
    // assert_eq!(f0_reader.get(4), Some(true));
    // assert_eq!(f1_reader.get(4), Some(false));
}
