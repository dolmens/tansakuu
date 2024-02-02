use std::{
    collections::{hash_map::RandomState, BTreeSet, HashMap},
    sync::Arc,
};

use crate::{
    document::{OwnedValue, Value},
    index::IndexWriter,
    postings::{BuildingPostingList, BuildingPostingWriter, PostingFormat},
    schema::Index,
    util::{
        ha3_capacity_policy::Ha3CapacityPolicy, hash::hash_string_64,
        layered_hashmap::LayeredHashMapWriter,
    },
    DocId, HASHMAP_INITIAL_CAPACITY,
};

use super::InvertedIndexBuildingSegmentData;

pub type PostingTable =
    LayeredHashMapWriter<u64, BuildingPostingList, RandomState, Ha3CapacityPolicy>;

pub struct InvertedIndexWriter {
    index: Index,
    posting_table: PostingTable,
    posting_writers: Vec<BuildingPostingWriter>,
    posting_indexes: HashMap<u64, usize>,
    modified_postings: BTreeSet<usize>,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
}

impl InvertedIndexWriter {
    pub fn new(index: Index) -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = Ha3CapacityPolicy;
        let posting_table = PostingTable::with_initial_capacity(
            HASHMAP_INITIAL_CAPACITY,
            hasher_builder,
            capacity_policy,
        );
        let postings = posting_table.hashmap();

        Self {
            index,
            posting_table,
            posting_writers: Vec::new(),
            posting_indexes: HashMap::new(),
            modified_postings: BTreeSet::new(),
            index_data: Arc::new(InvertedIndexBuildingSegmentData::new(postings)),
        }
    }
}

impl IndexWriter for InvertedIndexWriter {
    fn add_field(&mut self, field: &str, value: OwnedValue) {
        for (pos, tok) in (&value).as_str().unwrap().split_whitespace().enumerate() {
            let hashkey = hash_string_64(tok);
            let writer_index = self
                .posting_indexes
                .entry(hashkey)
                .or_insert_with(|| {
                    let posting_format = PostingFormat::builder()
                        .with_tflist()
                        .with_position_list()
                        .build();
                    let posting_writer = BuildingPostingWriter::new(posting_format, 1024);
                    let building_posting_list = posting_writer.building_posting_list().clone();
                    self.posting_table.insert(hashkey, building_posting_list);
                    self.posting_writers.push(posting_writer);
                    self.posting_writers.len() - 1
                })
                .clone();
            let posting_writer = &mut self.posting_writers[writer_index];
            let field_offset = self.index.field_offset(field);
            posting_writer.add_pos(field_offset, pos as u32).unwrap();
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
