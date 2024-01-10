use std::sync::Arc;

use crate::{document::Value, index::IndexWriter, DocId};

use super::PrimaryKeyIndexBuildingSegmentData;

pub struct PrimaryKeyIndexWriter {
    key: Option<String>,
    index_data: Arc<PrimaryKeyIndexBuildingSegmentData>,
}

impl PrimaryKeyIndexWriter {
    pub fn new() -> Self {
        Self {
            key: None,
            index_data: Arc::new(PrimaryKeyIndexBuildingSegmentData::new()),
        }
    }
}

impl IndexWriter for PrimaryKeyIndexWriter {
    fn add_field(&mut self, _field: &str, value: &Value) {
        assert!(self.key.is_none());
        self.key = Some(value.to_string());
    }

    fn end_document(&mut self, docid: DocId) {
        assert!(self.key.is_some());
        unsafe {
            self.index_data.insert(self.key.take().unwrap(), docid);
        }
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
