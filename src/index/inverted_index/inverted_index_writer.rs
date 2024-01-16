use std::{collections::HashMap, sync::Arc};

use crate::{
    document::Value,
    index::IndexWriter,
    postings::{BuildingPostingWriter, PostingFormat},
    DocId,
};

use super::InvertedIndexBuildingSegmentData;

pub struct InvertedIndexWriter {
    fields: HashMap<String, String>,
    posting_writers: HashMap<String, BuildingPostingWriter>,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
}

impl InvertedIndexWriter {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            posting_writers: HashMap::new(),
            index_data: Arc::new(InvertedIndexBuildingSegmentData::new()),
        }
    }
}

impl IndexWriter for InvertedIndexWriter {
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
            let posting_writer = self
                .posting_writers
                .entry(tok.to_string())
                .or_insert_with(|| {
                    let posting_format = PostingFormat::default();
                    let posting_writer = BuildingPostingWriter::new(posting_format, 1024);
                    let building_posting_list = posting_writer.building_posting_list();
                    unsafe {
                        self.index_data
                            .postings
                            .insert(tok.to_string(), building_posting_list);
                    }
                    posting_writer
                });
            posting_writer.add_pos(0);
            posting_writer.end_doc(docid);
        }
        self.fields.clear();
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
