use crate::{
    table::SegmentId,
    util::{FixedCapacityPolicy, LayeredHashMap},
    DocId,
};

use std::{
    collections::{hash_map::RandomState, HashMap, HashSet},
    fs,
    path::Path,
};

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
        let deleted = self.to_normal_hashmap();
        let json = serde_json::to_string_pretty(&deleted).unwrap();
        fs::write(path, json).unwrap();
    }

    fn to_normal_hashmap(&self) -> HashMap<SegmentId, HashSet<DocId>> {
        let mut deleted = HashMap::<SegmentId, HashSet<DocId>>::new();
        for (seg, docs) in self.deleted.iter() {
            let docs: HashSet<_> = docs.iter().map(|(&x, _)| x).collect();
            deleted.insert(seg.clone(), docs);
        }
        deleted
    }
}
