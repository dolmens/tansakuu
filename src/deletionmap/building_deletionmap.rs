use crate::{
    table::SegmentId,
    util::{FixedCapacityPolicy, LayeredHashMap},
    DocId,
};

use std::{collections::hash_map::RandomState, fs::File, path::Path};

use super::DeletionDictBuilder;

pub struct BuildingDeletionMap {
    deleted: LayeredHashMap<SegmentId, LayeredHashMap<DocId, ()>>,
}

impl BuildingDeletionMap {
    pub fn new() -> Self {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let d2 = LayeredHashMap::with_initial_capacity(1024, hasher_builder, capacity_policy);

        Self { deleted: d2 }
    }

    pub fn is_deleted(&self, segment_id: &SegmentId, docid: DocId) -> bool {
        self.deleted
            .get(segment_id)
            .map_or(false, |seg| seg.contains_key(&docid))
    }

    pub unsafe fn delete_doc(&self, segment_id: SegmentId, docid: DocId) {
        if self.deleted.get(&segment_id).is_none() {
            let hasher_builder = RandomState::new();
            let capacity_policy = FixedCapacityPolicy;
            let docset =
                LayeredHashMap::with_initial_capacity(1024, hasher_builder, capacity_policy);

            self.deleted.insert(segment_id.clone(), docset);
        }

        let docset = self.deleted.get(&segment_id).unwrap();
        docset.insert(docid, ());
    }

    pub fn is_empty(&self) -> bool {
        self.deleted.is_empty()
    }

    pub fn save(&self, path: impl AsRef<Path>) {
        let file = File::create(path).unwrap();
        let mut dict_builder = DeletionDictBuilder::new(file);
        let mut keybuf = [0_u8; 36];
        for (seg, docs) in self.deleted.iter() {
            keybuf[..32].copy_from_slice(seg.as_bytes());
            for (&docid, _) in docs.iter() {
                keybuf[32..36].copy_from_slice(&docid.to_be_bytes());
                dict_builder.insert(&keybuf).unwrap();
            }
        }
        dict_builder.finish().unwrap();
    }
}
