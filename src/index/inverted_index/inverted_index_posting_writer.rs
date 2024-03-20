use std::{cell::RefCell, collections::HashMap, hash::RandomState, rc::Rc};

use crate::{
    postings::{BuildingPostingList, BuildingPostingWriter, PostingFormat},
    util::{
        ha3_capacity_policy::Ha3CapacityPolicy,
        layered_hashmap::{LayeredHashMap, LayeredHashMapWriter},
    },
    DocId, HASHMAP_INITIAL_CAPACITY,
};

pub type BuildingPostingTable =
    LayeredHashMapWriter<u64, BuildingPostingList, RandomState, Ha3CapacityPolicy>;
pub type BuildingPostingData = LayeredHashMap<u64, BuildingPostingList>;

pub struct InvertedIndexPostingWriter {
    posting_table: BuildingPostingTable,
    posting_writers: HashMap<u64, Rc<RefCell<BuildingPostingWriter>>>,
    modified_postings: HashMap<u64, Rc<RefCell<BuildingPostingWriter>>>,
    posting_format: PostingFormat,
}

impl InvertedIndexPostingWriter {
    pub fn new(posting_format: PostingFormat, hashmap_initial_capacity: usize) -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = Ha3CapacityPolicy;
        let hashmap_initial_capacity = if hashmap_initial_capacity > 0 {
            hashmap_initial_capacity
        } else {
            HASHMAP_INITIAL_CAPACITY
        };
        let posting_table = BuildingPostingTable::with_capacity(
            hashmap_initial_capacity,
            hasher_builder,
            capacity_policy,
        );

        Self {
            posting_table,
            posting_writers: HashMap::new(),
            modified_postings: HashMap::new(),
            posting_format,
        }
    }

    pub fn posting_data(&self) -> BuildingPostingData {
        self.posting_table.hashmap()
    }

    pub fn posting_format(&self) -> &PostingFormat {
        &self.posting_format
    }

    pub fn add_token(&mut self, hash: u64, field_offset: usize) {
        self.add_token_with_position(hash, field_offset, 0);
    }

    pub fn add_token_with_position(&mut self, hash: u64, field_offset: usize, pos: u32) {
        let posting_writer = self
            .posting_writers
            .entry(hash)
            .or_insert_with(|| {
                let posting_writer = Rc::new(RefCell::new(BuildingPostingWriter::new(
                    self.posting_format.clone(),
                )));
                let building_posting_list = posting_writer.borrow().building_posting_list().clone();
                self.posting_table.insert(hash, building_posting_list);
                posting_writer
            })
            .clone();
        self.modified_postings
            .entry(hash)
            .or_insert_with(|| posting_writer.clone());

        posting_writer
            .borrow_mut()
            .add_pos(field_offset, pos)
            .unwrap();
    }

    pub fn end_document(&mut self, docid: DocId) {
        for (_, posting_writer) in &self.modified_postings {
            posting_writer.borrow_mut().end_doc(docid).unwrap();
        }
        self.modified_postings.clear();
    }
}
