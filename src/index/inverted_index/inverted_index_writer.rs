use std::{
    collections::{hash_map::RandomState, BTreeSet, HashMap},
    sync::Arc,
};

use crate::{
    document::Value,
    index::IndexWriter,
    postings::{BuildingPostingList, BuildingPostingWriter, PostingFormat},
    util::{capacity_policy::FixedCapacityPolicy, layered_hashmap::LayeredHashMapWriter},
    DocId,
};

use super::InvertedIndexBuildingSegmentData;

pub struct InvertedIndexWriter {
    posting_table: LayeredHashMapWriter<String, BuildingPostingList>,
    posting_writers: Vec<BuildingPostingWriter>,
    posting_indexes: HashMap<String, usize>,
    modified_postings: BTreeSet<usize>,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
}

impl InvertedIndexWriter {
    pub fn new() -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let posting_table =
            LayeredHashMapWriter::with_initial_capacity(1024, hasher_builder, capacity_policy);
        let postings = posting_table.hashmap();

        Self {
            posting_table,
            posting_writers: Vec::new(),
            posting_indexes: HashMap::new(),
            modified_postings: BTreeSet::new(),
            index_data: Arc::new(InvertedIndexBuildingSegmentData::new(postings)),
        }
    }
}

impl IndexWriter for InvertedIndexWriter {
    fn add_field(&mut self, _field: &str, value: &Value) {
        for (pos, tok) in value.to_string().split_whitespace().enumerate() {
            let writer_index = self
                .posting_indexes
                .entry(tok.to_string())
                .or_insert_with(|| {
                    let posting_format = PostingFormat::builder()
                        .with_tflist()
                        .with_position_list()
                        .build();
                    let posting_writer = BuildingPostingWriter::new(posting_format, 1024);
                    let building_posting_list = posting_writer.building_posting_list().clone();
                    self.posting_table
                        .insert(tok.to_string(), building_posting_list);
                    self.posting_writers.push(posting_writer);
                    self.posting_writers.len() - 1
                })
                .clone();
            let posting_writer = &mut self.posting_writers[writer_index];
            posting_writer.add_pos(0, pos as u32).unwrap();
            self.modified_postings.insert(writer_index);
        }
    }

    fn end_document(&mut self, docid: DocId) {
        for &writer_index in &self.modified_postings {
            let posting_writer = &mut self.posting_writers[writer_index];
            posting_writer.end_doc(docid).unwrap();
        }
        self.modified_postings.clear();
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
