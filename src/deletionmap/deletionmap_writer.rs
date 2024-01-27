use std::collections::{hash_map::RandomState, HashMap};

use crate::{
    table::SegmentId,
    util::{FixedCapacityPolicy, LayeredHashMap, LayeredHashMapWriter},
    DocId,
};

use super::BuildingDeletionMap;

pub struct DeletionMapWriter {
    writers: HashMap<SegmentId, LayeredHashMapWriter<DocId, ()>>,
    deletionmap: LayeredHashMapWriter<SegmentId, LayeredHashMap<DocId, ()>>,
}

impl DeletionMapWriter {
    pub fn new() -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let deletionmap =
            LayeredHashMapWriter::with_initial_capacity(1024, hasher_builder, capacity_policy);
        Self {
            writers: HashMap::new(),
            deletionmap,
        }
    }

    pub fn deletionmap(&self) -> BuildingDeletionMap {
        BuildingDeletionMap::new(self.deletionmap.hashmap())
    }

    pub fn is_deleted(&self, segment_id: &SegmentId, docid: DocId) -> bool {
        self.writers
            .get(segment_id)
            .map_or(false, |seg| seg.contains_key(&docid))
    }

    pub fn delete_doc(&mut self, segment_id: SegmentId, docid: DocId) {
        self.writers
            .entry(segment_id.clone())
            .or_insert_with(|| {
                let hasher_builder = RandomState::new();
                let capacity_policy = FixedCapacityPolicy;
                let writer =
                    LayeredHashMapWriter::with_initial_capacity(4, hasher_builder, capacity_policy);
                let data = writer.hashmap();
                self.deletionmap.insert(segment_id, data);
                writer
            })
            .insert(docid, ());
    }

    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }
}
