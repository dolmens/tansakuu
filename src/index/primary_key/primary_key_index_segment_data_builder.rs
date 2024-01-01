use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

use crate::{index::IndexSegmentDataBuilder, schema::Index, DocId};

use super::PrimaryKeyIndexPersistentSegmentData;

pub struct PrimaryKeyIndexSegmentDataBuilder {}

impl PrimaryKeyIndexSegmentDataBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl IndexSegmentDataBuilder for PrimaryKeyIndexSegmentDataBuilder {
    fn build(
        &self,
        index: &Index,
        path: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let _ = index;
        let mut keys = HashMap::new();
        let file = File::open(path).unwrap();
        let file_reader = BufReader::new(file);
        for line in file_reader.lines() {
            let line = line.unwrap();
            let mut key_and_docid = line.split_whitespace();
            let key = key_and_docid.next().unwrap();
            let docid = key_and_docid.next().unwrap().parse::<DocId>().unwrap();
            keys.insert(key.to_string(), docid);
        }

        Box::new(PrimaryKeyIndexPersistentSegmentData::new(keys))
    }
}
