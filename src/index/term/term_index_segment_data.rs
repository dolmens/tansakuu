use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    index::{IndexSegmentData, IndexSegmentReader, SegmentPosting},
    DocId,
};

pub struct TermIndexSegmentData {
    postings: Mutex<HashMap<String, Vec<DocId>>>,
}

pub struct TermIndexSegmentReader {
    index_data: Arc<TermIndexSegmentData>,
}

impl IndexSegmentData for TermIndexSegmentData {}

impl TermIndexSegmentReader {
    pub fn new(index_data: Arc<TermIndexSegmentData>) -> Self {
        Self { index_data }
    }
}

impl IndexSegmentReader for TermIndexSegmentReader {
    fn segment_posting(&self, key: &str) -> crate::index::SegmentPosting {
        let postings = self.index_data.postings.lock().unwrap();
        let docids = postings.get(key).cloned().unwrap_or_default();
        SegmentPosting { docids }
    }
}
