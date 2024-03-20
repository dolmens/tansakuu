use tansakuu::{
    columnar::GeoLocationColumnReader,
    doc,
    query::Term,
    schema::{FieldType, Schema, COLUMNAR, INDEXED},
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
fn test_geo_location() {
    let mut schema_builder = Schema::builder();
    schema_builder.add_field("f0".to_string(), FieldType::GeoLocation, COLUMNAR | INDEXED);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Table::create(schema, settings);

    let mut writer = table.writer();

    // Hangzhou
    let (lon0, lat0) = (120.15, 30.28);
    writer.add_document(doc!(f0 => vec![lon0, lat0]));
    writer.add_document(doc!(f0 => ""));
    // Shanghai
    let (lon1, lat1) = (121.47, 31.23);
    writer.add_document(doc!(f0 => format!("{}, {}", lon1, lat1)));

    let reader = table.reader();

    let column_reader = reader
        .column_reader()
        .column("f0")
        .unwrap()
        .downcast_ref::<GeoLocationColumnReader>()
        .unwrap();
    assert_eq!(column_reader.get(0), Some((lon0, lat0)));
    assert_eq!(column_reader.get(1), None);
    assert_eq!(column_reader.get(2), Some((lon1, lat1)));

    let index_reader = reader.index_reader();

    // Kunshan
    let (lon2, lat2) = (120.98, 31.38);
    let dist1 = 20_000;
    let term1 = Term::new("f0".to_string(), format!("{}, {}; {}", lon2, lat2, dist1));
    let docids = get_all_docs(index_reader, &term1);
    assert_eq!(docids, vec![2]);

    // Serialize
    writer.new_segment();

    // Shaoxing
    let (lon3, lat3) = (120.47, 30.08);
    writer.add_document(doc!(f0 => vec![lon3, lat3]));

    let reader = table.reader();

    let column_reader = reader
        .column_reader()
        .column("f0")
        .unwrap()
        .downcast_ref::<GeoLocationColumnReader>()
        .unwrap();
    assert_eq!(column_reader.get(0), Some((lon0, lat0)));
    assert_eq!(column_reader.get(1), None);
    assert_eq!(column_reader.get(2), Some((lon1, lat1)));
    assert_eq!(column_reader.get(3), Some((lon3, lat3)));

    let index_reader = reader.index_reader();

    let term1 = Term::new("f0".to_string(), format!("{}, {}; {}", lon2, lat2, dist1));
    let docids = get_all_docs(index_reader, &term1);
    assert_eq!(docids, vec![2]);

    let dist3 = 10_000;
    let term3 = Term::new("f0".to_string(), format!("{}, {}; {}", lon3, lat3, dist3));
    let docids = get_all_docs(index_reader, &term3);
    assert_eq!(docids, vec![0, 3]);

    // merge
    writer.new_segment();

    // Jiaxing
    let (lon4, lat4) = (120.75, 30.75);
    writer.add_document(doc!(f0 => vec![lon4, lat4]));

    let reader = table.reader();

    let column_reader = reader
        .column_reader()
        .column("f0")
        .unwrap()
        .downcast_ref::<GeoLocationColumnReader>()
        .unwrap();
    assert_eq!(column_reader.get(0), Some((lon0, lat0)));
    assert_eq!(column_reader.get(1), None);
    assert_eq!(column_reader.get(2), Some((lon1, lat1)));
    assert_eq!(column_reader.get(3), Some((lon3, lat3)));
    assert_eq!(column_reader.get(4), Some((lon4, lat4)));

    let index_reader = reader.index_reader();

    let term1 = Term::new("f0".to_string(), format!("{}, {}; {}", lon2, lat2, dist1));
    let docids = get_all_docs(index_reader, &term1);
    assert_eq!(docids, vec![2]);

    let dist3 = 10_000;
    let term3 = Term::new("f0".to_string(), format!("{}, {}; {}", lon3, lat3, dist3));
    let docids = get_all_docs(index_reader, &term3);
    assert_eq!(docids, vec![0, 3]);

    let dist4 = 400_000;
    let term4 = Term::new("f0".to_string(), format!("{}, {}; {}", lon3, lat4, dist4));
    let docids = get_all_docs(index_reader, &term4);
    assert_eq!(docids, vec![0, 2, 3, 4]);
}
