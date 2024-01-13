use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
};

use tantivy_common::file_slice::{FileSlice, WrapFile};

use crate::{index::IndexSegmentDataBuilder, postings::TermDict, schema::Index};

use super::InvertedIndexPersistentSegmentData;

pub struct InvertedIndexSegmentDataBuilder {}

impl InvertedIndexSegmentDataBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl IndexSegmentDataBuilder for InvertedIndexSegmentDataBuilder {
    fn build(
        &self,
        index: &Index,
        path: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let _ = index;
        let mut postings = HashMap::new();
        let file = File::open(path).unwrap();
        let file_reader = BufReader::new(file);
        for line in file_reader.lines() {
            let line = line.unwrap();
            let mut tok_and_docids = line.split_whitespace();
            let tok = tok_and_docids.next().unwrap();
            let docids: Vec<_> = tok_and_docids.map(|s| s.parse::<u32>().unwrap()).collect();
            postings.insert(tok.to_string(), docids);
        }

        let dict_path = path.join(".dict");
        let dict_file = File::open(dict_path).unwrap();
        let dict_data = FileSlice::new(Arc::new(WrapFile::new(dict_file).unwrap()));
        let term_dict = TermDict::open(dict_data).unwrap();

        let posting_path = path.join(".posting");
        let posting_file = File::open(posting_path).unwrap();
        let posting_data = FileSlice::new(Arc::new(WrapFile::new(posting_file).unwrap()));
        let posting_data = posting_data.read_bytes().unwrap();

        Box::new(InvertedIndexPersistentSegmentData::new(
            postings,
            term_dict,
            posting_data,
        ))
    }
}
