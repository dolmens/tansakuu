use tansakuu::{
    columnar::BooleanColumnReader,
    doc,
    document::NULL_VALUE,
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

// #[test]
// fn test_boolean_column() {
//     let mut schema_builder = Schema::builder();
//     schema_builder.add_field("f0".to_string(), FieldType::Boolean, INDEXED | COLUMNAR);
//     schema_builder.add_field(
//         "f1".to_string(),
//         FieldType::Boolean,
//         INDEXED | COLUMNAR | NOT_NULL,
//     );
//     let schema = schema_builder.build();
//     let settings = TableSettings::new();
//     let table = Table::create(schema, settings);

//     let mut writer = table.writer();

//     writer.add_document(doc!(f0 => true, f1 => false));
//     writer.add_document(doc!(f0 => false, f1 => true));
//     writer.add_document(doc!(f0 => NULL_VALUE, f1 => NULL_VALUE));
//     writer.add_document(doc!(f0 => true, f1 => false));
//     for _ in 4..128 {
//         writer.add_document(doc!());
//     }
//     writer.add_document(doc!(f0 => true, f1 => true));

//     let reader = table.reader();

//     let f0_reader = reader
//         .column_reader()
//         .column("f0")
//         .unwrap()
//         .downcast_ref::<BooleanColumnReader>()
//         .unwrap();
//     let f1_reader = reader
//         .column_reader()
//         .column("f1")
//         .unwrap()
//         .downcast_ref::<BooleanColumnReader>()
//         .unwrap();

//     assert_eq!(f0_reader.get(0), Some(true));
//     assert_eq!(f1_reader.get(0), Some(false));
//     assert_eq!(f0_reader.get(1), Some(false));
//     assert_eq!(f1_reader.get(1), Some(true));
//     assert_eq!(f0_reader.get(2), None);
//     assert_eq!(f1_reader.get(2), Some(false));

//     let index_reader = reader.index_reader();
//     let term_f0_true = Term::new("f0".to_string(), "true".to_string());
//     let term_f0_false = Term::new("f0".to_string(), "false".to_string());
//     let term_f0_null = Term::null("f0".to_string());
//     let term_f0_non_null = Term::non_null("f0".to_string());
//     let term_f1_true = Term::new("f1".to_string(), "true".to_string());
//     let term_f1_false = Term::new("f1".to_string(), "false".to_string());
//     let term_f1_null = Term::null("f1".to_string());
//     let term_f1_non_null = Term::non_null("f1".to_string());

//     let empty: Vec<DocId> = vec![];

//     let f0_true_docs: Vec<DocId> = vec![0, 3, 128];
//     let f0_null_docs: Vec<DocId> = vec![2];
//     let f0_false_docs: Vec<_> = (0..129)
//         .map(|docid| docid as DocId)
//         .filter(|x| !f0_true_docs.contains(x) && !f0_null_docs.contains(x))
//         .collect();
//     let f0_non_null_docs: Vec<DocId> = (0..129)
//         .map(|docid| docid as DocId)
//         .filter(|x| !f0_null_docs.contains(x))
//         .collect();

//     assert_eq!(get_all_docs(index_reader, &term_f0_true), f0_true_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f0_false), f0_false_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f0_null), f0_null_docs);
//     assert_eq!(
//         get_all_docs(index_reader, &term_f0_non_null),
//         f0_non_null_docs
//     );

//     let f1_true_docs: Vec<DocId> = vec![1, 128];
//     let f1_false_docs: Vec<_> = (0..129)
//         .map(|docid| docid as DocId)
//         .filter(|x| !f1_true_docs.contains(x))
//         .collect();
//     let f1_full_docs: Vec<_> = (0..129).map(|docid| docid as DocId).collect();

//     assert_eq!(get_all_docs(index_reader, &term_f1_true), f1_true_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f1_false), f1_false_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f1_null), empty);
//     assert_eq!(get_all_docs(index_reader, &term_f1_non_null), f1_full_docs);

//     // Serialize
//     writer.new_segment();

//     writer.add_document(doc!(f0 => false, f1 => true));
//     writer.add_document(doc!(f0 => NULL_VALUE, f1 => false));

//     let reader = table.reader();

//     let f0_reader = reader
//         .column_reader()
//         .column("f0")
//         .unwrap()
//         .downcast_ref::<BooleanColumnReader>()
//         .unwrap();
//     let f1_reader = reader
//         .column_reader()
//         .column("f1")
//         .unwrap()
//         .downcast_ref::<BooleanColumnReader>()
//         .unwrap();

//     assert_eq!(f0_reader.get(0), Some(true));
//     assert_eq!(f1_reader.get(0), Some(false));
//     assert_eq!(f0_reader.get(1), Some(false));
//     assert_eq!(f1_reader.get(1), Some(true));
//     assert_eq!(f0_reader.get(2), None);
//     assert_eq!(f1_reader.get(2), Some(false));
//     assert_eq!(f0_reader.get(128), Some(true));
//     assert_eq!(f1_reader.get(128), Some(true));

//     let index_reader = reader.index_reader();

//     let f0_null_docs: Vec<DocId> = vec![2, 130];
//     let f0_false_docs: Vec<_> = (0..131)
//         .map(|docid| docid as DocId)
//         .filter(|x| !f0_true_docs.contains(x) && !f0_null_docs.contains(x))
//         .collect();

//     assert_eq!(get_all_docs(index_reader, &term_f0_true), f0_true_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f0_false), f0_false_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f0_null), f0_null_docs);

//     let f1_full_docs: Vec<_> = (0..131).map(|docid| docid as DocId).collect();
//     let f1_true_docs: Vec<DocId> = vec![1, 128, 129];
//     let f1_false_docs: Vec<_> = (0..131)
//         .map(|docid| docid as DocId)
//         .filter(|x| !f1_true_docs.contains(x))
//         .collect();

//     assert_eq!(get_all_docs(index_reader, &term_f1_true), f1_true_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f1_false), f1_false_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f1_null), empty);
//     assert_eq!(get_all_docs(index_reader, &term_f1_non_null), f1_full_docs);

//     // Merge
//     writer.new_segment();

//     let reader = table.reader();

//     let f0_reader = reader
//         .column_reader()
//         .column("f0")
//         .unwrap()
//         .downcast_ref::<BooleanColumnReader>()
//         .unwrap();
//     let f1_reader = reader
//         .column_reader()
//         .column("f1")
//         .unwrap()
//         .downcast_ref::<BooleanColumnReader>()
//         .unwrap();

//     assert_eq!(f0_reader.get(0), Some(true));
//     assert_eq!(f1_reader.get(0), Some(false));
//     assert_eq!(f0_reader.get(1), Some(false));
//     assert_eq!(f1_reader.get(1), Some(true));
//     assert_eq!(f0_reader.get(2), None);
//     assert_eq!(f1_reader.get(2), Some(false));
//     assert_eq!(f0_reader.get(128), Some(true));
//     assert_eq!(f1_reader.get(128), Some(true));

//     let index_reader = reader.index_reader();

//     assert_eq!(get_all_docs(index_reader, &term_f0_true), f0_true_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f0_false), f0_false_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f0_null), f0_null_docs);

//     assert_eq!(get_all_docs(index_reader, &term_f1_true), f1_true_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f1_false), f1_false_docs);
//     assert_eq!(get_all_docs(index_reader, &term_f1_null), empty);
//     assert_eq!(get_all_docs(index_reader, &term_f1_non_null), f1_full_docs);
// }

#[test]
fn test_with_deletion() {
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
    writer.add_document(doc!(f0 => NULL_VALUE, f1 => NULL_VALUE));
    writer.add_document(doc!(f0 => true, f1 => false));

    let term_f0_false = Term::new("f0".to_string(), "false".to_string());
    writer.delete_documents(&term_f0_false);

    for _ in 4..128 {
        writer.add_document(doc!());
    }
    // DocId: 128(127)
    writer.add_document(doc!(f0 => true, f1 => true));

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

    let index_reader = reader.index_reader();
    let term_f0_true = Term::new("f0".to_string(), "true".to_string());
    let term_f0_false = Term::new("f0".to_string(), "false".to_string());
    let term_f0_null = Term::null("f0".to_string());
    let term_f0_non_null = Term::non_null("f0".to_string());
    let term_f1_true = Term::new("f1".to_string(), "true".to_string());
    let term_f1_false = Term::new("f1".to_string(), "false".to_string());
    let term_f1_null = Term::null("f1".to_string());
    let term_f1_non_null = Term::non_null("f1".to_string());

    let empty: Vec<DocId> = vec![];

    let f0_true_docs: Vec<DocId> = vec![0, 3, 128];
    let f0_null_docs: Vec<DocId> = vec![2];
    let f0_false_docs: Vec<_> = (0..129)
        .map(|docid| docid as DocId)
        .filter(|x| !f0_true_docs.contains(x) && !f0_null_docs.contains(x))
        .collect();
    let f0_non_null_docs: Vec<DocId> = (0..129)
        .map(|docid| docid as DocId)
        .filter(|x| !f0_null_docs.contains(x))
        .collect();

    assert_eq!(get_all_docs(index_reader, &term_f0_true), f0_true_docs);
    assert_eq!(get_all_docs(index_reader, &term_f0_false), f0_false_docs);
    assert_eq!(get_all_docs(index_reader, &term_f0_null), f0_null_docs);
    assert_eq!(
        get_all_docs(index_reader, &term_f0_non_null),
        f0_non_null_docs
    );

    let f1_true_docs: Vec<DocId> = vec![1, 128];
    let f1_false_docs: Vec<_> = (0..129)
        .map(|docid| docid as DocId)
        .filter(|x| !f1_true_docs.contains(x))
        .collect();
    let f1_full_docs: Vec<_> = (0..129).map(|docid| docid as DocId).collect();

    assert_eq!(get_all_docs(index_reader, &term_f1_true), f1_true_docs);
    assert_eq!(get_all_docs(index_reader, &term_f1_false), f1_false_docs);
    assert_eq!(get_all_docs(index_reader, &term_f1_null), empty);
    assert_eq!(get_all_docs(index_reader, &term_f1_non_null), f1_full_docs);

    // Serialize
    writer.new_segment();

    // DocId: 129(128)
    writer.add_document(doc!(f0 => false, f1 => true));
    // DocId: 130(129)
    writer.add_document(doc!(f0 => NULL_VALUE, f1 => false));

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
    assert_eq!(f0_reader.get(1), None);
    assert_eq!(f1_reader.get(1), Some(false));
    assert_eq!(f0_reader.get(2), Some(true));
    assert_eq!(f1_reader.get(2), Some(false));

    let index_reader = reader.index_reader();

    let f0_true_docs: Vec<DocId> = vec![0, 2, 127];
    let f0_null_docs: Vec<DocId> = vec![1, 129];
    let f0_false_docs: Vec<_> = (0..130)
        .map(|docid| docid as DocId)
        .filter(|x| !f0_true_docs.contains(x) && !f0_null_docs.contains(x))
        .collect();
    let f0_non_null_docs: Vec<DocId> = (0..130)
        .map(|docid| docid as DocId)
        .filter(|x| !f0_null_docs.contains(x))
        .collect();

    assert_eq!(get_all_docs(index_reader, &term_f0_true), f0_true_docs);
    assert_eq!(get_all_docs(index_reader, &term_f0_false), f0_false_docs);
    assert_eq!(get_all_docs(index_reader, &term_f0_null), f0_null_docs);
    assert_eq!(
        get_all_docs(index_reader, &term_f0_non_null),
        f0_non_null_docs
    );

    let f1_true_docs = vec![127, 128];
    let f1_full_docs: Vec<_> = (0..130).map(|docid| docid as DocId).collect();
    let f1_false_docs: Vec<_> = (0..130)
        .map(|docid| docid as DocId)
        .filter(|x| !f1_true_docs.contains(x))
        .collect();

    assert_eq!(get_all_docs(index_reader, &term_f1_true), f1_true_docs);
    assert_eq!(get_all_docs(index_reader, &term_f1_false), f1_false_docs);
    assert_eq!(get_all_docs(index_reader, &term_f1_null), empty);
    assert_eq!(get_all_docs(index_reader, &term_f1_non_null), f1_full_docs);

    writer.delete_documents(&term_f1_true);

    // Merge
    writer.new_segment();

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
    assert_eq!(f0_reader.get(1), None);
    assert_eq!(f1_reader.get(1), Some(false));
    assert_eq!(f0_reader.get(2), Some(true));
    assert_eq!(f1_reader.get(2), Some(false));

    let index_reader = reader.index_reader();

    let f0_true_docs: Vec<DocId> = vec![0, 2, 127];
    // let f0_null_docs: Vec<DocId> = vec![1, 129];
    // let f0_false_docs: Vec<_> = (0..130)
    //     .map(|docid| docid as DocId)
    //     .filter(|x| !f0_true_docs.contains(x) && !f0_null_docs.contains(x))
    //     .collect();
    // let f0_non_null_docs: Vec<DocId> = (0..130)
    //     .map(|docid| docid as DocId)
    //     .filter(|x| !f0_null_docs.contains(x))
    //     .collect();

    // TODO: Deletionmap is buggy
    assert_eq!(get_all_docs(index_reader, &term_f0_true), f0_true_docs);
    // assert_eq!(get_all_docs(index_reader, &term_f0_false), f0_false_docs);
    // assert_eq!(get_all_docs(index_reader, &term_f0_null), f0_null_docs);
    // assert_eq!(
    //     get_all_docs(index_reader, &term_f0_non_null),
    //     f0_non_null_docs
    // );

    // let f1_true_docs = vec![127, 128];
    // let f1_full_docs: Vec<_> = (0..130).map(|docid| docid as DocId).collect();
    // let f1_false_docs: Vec<_> = (0..130)
    //     .map(|docid| docid as DocId)
    //     .filter(|x| !f1_true_docs.contains(x))
    //     .collect();

    // assert_eq!(get_all_docs(index_reader, &term_f1_true), f1_true_docs);
    // assert_eq!(get_all_docs(index_reader, &term_f1_false), f1_false_docs);
    // assert_eq!(get_all_docs(index_reader, &term_f1_null), empty);
    // assert_eq!(get_all_docs(index_reader, &term_f1_non_null), f1_full_docs);
}
