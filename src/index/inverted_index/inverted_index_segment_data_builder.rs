use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

use tantivy_common::OwnedBytes;

use crate::{index::IndexSegmentDataBuilder, schema::Index};

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

        let bytes = OwnedBytes::empty();
        Box::new(InvertedIndexPersistentSegmentData::new(postings, bytes))
    }
}
