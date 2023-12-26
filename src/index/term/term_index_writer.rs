use std::{collections::HashMap, sync::Arc};

use crate::{index::IndexWriter, DocId, document::Value};

use super::TermIndexBuildingSegmentData;

pub struct TermIndexWriter {
    fields: HashMap<String, String>,
    index_data: Arc<TermIndexBuildingSegmentData>,
}

impl TermIndexWriter {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            index_data: Arc::new(TermIndexBuildingSegmentData::new()),
        }
    }
}

impl IndexWriter for TermIndexWriter {
    fn add_field(&mut self, field: &str, value: &Value) {
        self.fields.insert(field.to_string(), value.to_string());
    }

    fn end_document(&mut self, docid: DocId) {
        let mut keywords = HashMap::new();
        for (field, value) in &self.fields {
            for tok in value.split_whitespace() {
                keywords.entry(tok).or_insert_with(Vec::new).push(field);
            }
        }
        for (tok, _fields) in keywords.iter() {
            self.index_data.add_doc(tok.to_string(), docid);
        }
        self.fields.clear();
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
