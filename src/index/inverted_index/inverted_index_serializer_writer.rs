use std::{
    fs::File,
    io::{self, Write},
    path::Path,
};

use crate::{
    postings::{TermDictBuilder, TermInfo},
    DocId,
};

pub struct InvertedIndexSerializerWriter {
    current_term_key: Option<String>,
    current_term_info: Option<TermInfo>,
    file: File,
    term_dict_builder: TermDictBuilder<File>,
    posting_file: File,
}

impl InvertedIndexSerializerWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let dict_path = path.as_ref().join(".dict");
        let dict_file = File::create(dict_path).unwrap();

        let posting_path = path.as_ref().join(".posting");
        let posting_file = File::create(posting_path).unwrap();

        Self {
            current_term_key: None,
            current_term_info: None,
            file: File::create(path).unwrap(),
            term_dict_builder: TermDictBuilder::new(dict_file),
            posting_file,
        }
    }

    pub fn start_term(&mut self, term: String) {
        assert!(self.current_term_key.is_none());
        write!(&mut self.file, "{}", &term).unwrap();
        self.current_term_key = Some(term);

        assert!(self.current_term_info.is_none());
    }

    pub fn add_doc(&mut self, term: &str, docid: DocId) {
        write!(&mut self.file, " {}", docid).unwrap();

        if let (Some(term_key), Some(term_info)) = (&self.current_term_key, &self.current_term_info)
        {
            assert_eq!(term_key, term);
            // self.posting_file.wri
        } else {
            panic!("invalid add_doc calling.");
        }
    }

    pub fn end_term(&mut self, term: &str) {
        writeln!(self.file).unwrap();

        if let (Some(term_key), Some(term_info)) = (&self.current_term_key, &self.current_term_info)
        {
            self.term_dict_builder.insert(term_key, term_info).unwrap();
            self.current_term_key = None;
            self.current_term_info = None;
        } else {
            panic!("invalid end_term calling.");
        }
    }

    pub fn finish(self) -> io::Result<()> {
        let _ = self.term_dict_builder.finish()?;

        Ok(())
    }
}
