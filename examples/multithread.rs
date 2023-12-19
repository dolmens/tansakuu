use std::{sync::Arc, thread, time::Duration};

use rindex::{
    document::Document,
    index::PostingIterator,
    query::Term,
    schema::{IndexType, Schema},
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

pub fn main() {
    let mut schema = Schema::new();
    schema.add_index(
        "title".to_string(),
        IndexType::Term,
        vec!["title".to_string()],
    );
    let settings = TableSettings::new();
    let table = Arc::new(Table::open_in(schema, settings, "."));

    let table_ref = table.clone();
    let writer = thread::spawn(move || {
        let mut writer = table_ref.writer();

        let mut doc1 = Document::new();
        doc1.add_field("title".to_string(), "hello world".to_string());
        writer.add_doc(&doc1);

        let mut doc2 = Document::new();
        doc2.add_field("title".to_string(), "world peace".to_string());
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

                break;
            }
        }

        thread::sleep(Duration::from_millis(1));
    });

    writer.join().unwrap();
    reader.join().unwrap();
}
