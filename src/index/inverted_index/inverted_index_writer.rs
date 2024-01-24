use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use crate::{
    document::Value,
    index::IndexWriter,
    postings::{BuildingPostingWriter, PostingFormat},
    DocId,
};

use super::InvertedIndexBuildingSegmentData;

pub struct InvertedIndexWriter {
    posting_writers: Vec<BuildingPostingWriter>,
    writer_indexes: HashMap<String, usize>,
    modified_writers: BTreeSet<usize>,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
}

impl InvertedIndexWriter {
    pub fn new() -> Self {
        Self {
            posting_writers: Vec::new(),
            writer_indexes: HashMap::new(),
            modified_writers: BTreeSet::new(),
            index_data: Arc::new(InvertedIndexBuildingSegmentData::new()),
        }
    }
}

impl IndexWriter for InvertedIndexWriter {
    fn add_field(&mut self, _field: &str, value: &Value) {
        for (pos, tok) in value.to_string().split_whitespace().enumerate() {
            let writer_index = self
                .writer_indexes
                .entry(tok.to_string())
                .or_insert_with(|| {
                    let posting_format = PostingFormat::builder()
                        .with_tflist()
                        .with_position_list()
                        .build();
                    let posting_writer = BuildingPostingWriter::new(posting_format, 1024);
                    let building_posting_list = posting_writer.building_posting_list().clone();
                    unsafe {
                        self.index_data
                            .postings
                            .insert(tok.to_string(), building_posting_list);
                    }
                    self.posting_writers.push(posting_writer);
                    self.posting_writers.len() - 1
                })
                .clone();
            let posting_writer = &mut self.posting_writers[writer_index];
            posting_writer.add_pos(0, pos as u32).unwrap();
            self.modified_writers.insert(writer_index);
        }
    }

    fn end_document(&mut self, docid: DocId) {
        for &writer_index in &self.modified_writers {
            let posting_writer = &mut self.posting_writers[writer_index];
            posting_writer.end_doc(docid).unwrap();
        }
        self.modified_writers.clear();
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
