use std::{collections::HashMap, sync::Arc};

use crate::{
    document::Value,
    index::IndexWriter,
    postings::{BuildingDocListWriter, DocListFormat},
    DocId,
};

use super::InvertedIndexBuildingSegmentData;

pub struct InvertedIndexWriter {
    fields: HashMap<String, String>,
    doc_list_writers: HashMap<String, BuildingDocListWriter>,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
}

impl InvertedIndexWriter {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            doc_list_writers: HashMap::new(),
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
            let doc_list_writer =
                self.doc_list_writers
                    .entry(tok.to_string())
                    .or_insert_with(|| {
                        let doc_list_format = DocListFormat::default();
                        let doc_list_writer = BuildingDocListWriter::new(doc_list_format, 1024);
                        let building_doc_list = doc_list_writer.building_doc_list();
                        unsafe {
                            self.index_data
                                .postings
                                .insert(tok.to_string(), building_doc_list);
                        }
                        doc_list_writer
                    });
            doc_list_writer.add_pos(0);
            doc_list_writer.end_doc(docid);
        }
        self.fields.clear();
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
