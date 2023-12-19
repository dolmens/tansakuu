use std::sync::Arc;

use crate::{index::IndexWriter, DocId};

use super::UniqueKeyIndexSegmentData;

pub struct UniqueKeyIndexWriter {
    key: Option<String>,
    index_data: Arc<UniqueKeyIndexSegmentData>,
}

impl UniqueKeyIndexWriter {
    pub fn new() -> Self {
        Self {
            key: None,
            index_data: Arc::new(UniqueKeyIndexSegmentData::new()),
        }
    }
}

impl IndexWriter for UniqueKeyIndexWriter {
    fn add_field(&mut self, _field: &str, value: &str) {
        assert!(self.key.is_none());
        self.key = Some(value.into());
    }

    fn end_document(&mut self, docid: DocId) {
        assert!(self.key.is_some());
        self.index_data.insert(self.key.take().unwrap(), docid);
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
