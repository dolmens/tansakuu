use std::{sync::Arc, thread, time::Duration};

use tansakuu::{
    document::InputDocument,
    index::PostingIterator,
    query::Term,
    schema::{SchemaBuilder, COLUMN, INDEXED},
    table::{Table, TableSettings},
    DocId, END_DOCID,
};

fn get_all_docs(posting_iter: &mut dyn PostingIterator) -> Vec<DocId> {
    let mut docids = vec![];
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

    docids
}

pub fn main() {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_text_field("title".to_string(), COLUMN | INDEXED);
    let schema = schema_builder.build();
    let settings = TableSettings::new();
    let table = Arc::new(Table::create(schema, settings));

    let table_ref = table.clone();
    let writer = thread::spawn(move || {
        let mut writer = table_ref.writer();

        let mut doc1 = InputDocument::new();
        doc1.add_field("title".to_string(), "hello world");
        writer.add_doc(&doc1);

        let mut doc2 = InputDocument::new();
        doc2.add_field("title".to_string(), "world peace");
        writer.add_doc(&doc2);
    });

    let table_ref = table.clone();
    let reader = thread::spawn(move || loop {
        let reader = table_ref.reader();
        let index_reader = reader.index_reader();

        let term = Term::new("title".to_string(), "peace".to_string());
        let posting_iter = index_reader.lookup(&term);
        if posting_iter.is_some() {
            let mut posting_iter = posting_iter.unwrap();
            let docids = get_all_docs(&mut *posting_iter);
            if docids == vec![1] {
                let term = Term::new("title".to_string(), "hello".to_string());
                let mut posting_iter = index_reader.lookup(&term).unwrap();
                let docids = get_all_docs(&mut *posting_iter);
                assert_eq!(docids, vec![0]);

                let term = Term::new("title".to_string(), "world".to_string());
                let mut posting_iter = index_reader.lookup(&term).unwrap();
                let docids = get_all_docs(&mut *posting_iter);
                assert_eq!(docids, vec![0, 1]);

                let column_reader = reader.column_reader();
                let title_column_reader = column_reader.typed_column::<String>("title").unwrap();
                assert_eq!(title_column_reader.get(0), Some("hello world".to_string()));
                assert_eq!(title_column_reader.get(1), Some("world peace".to_string()));

                break;
            }
        }

        thread::sleep(Duration::from_millis(1));
    });

    writer.join().unwrap();
    reader.join().unwrap();
}
