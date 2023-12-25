use std::{fs::File, io::Write, path::Path};

use crate::DocId;

pub struct TermIndexSerializerWriter {
    current_term: Option<String>,
    file: File,
}

impl TermIndexSerializerWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            current_term: None,
            file: File::create(path).unwrap(),
        }
    }

    pub fn start_term(&mut self, term: String) {
        assert!(self.current_term.is_none());
        write!(&mut self.file, "{}", &term).unwrap();
        self.current_term = Some(term);
    }

    pub fn add_doc(&mut self, term: &str, docid: DocId) {
        assert_eq!(self.current_term.as_deref(), Some(term));
        write!(&mut self.file, " {}", docid).unwrap();
    }

    pub fn end_term(&mut self, term: &str) {
        assert_eq!(self.current_term.as_deref(), Some(term));
        self.current_term = None;
        writeln!(self.file).unwrap();
    }
}
