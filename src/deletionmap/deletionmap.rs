use crate::{table::SegmentId, DocId};

use serde::{Deserialize, Serialize};

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct DeletionMap {
    deleted: HashMap<SegmentId, HashSet<DocId>>,
}

impl DeletionMap {
    pub fn load(path: impl AsRef<Path>) -> Self {
        let json = fs::read_to_string(path.as_ref()).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    pub fn save(&self, path: impl AsRef<Path>) {
        let json = serde_json::to_string_pretty(self).unwrap();
        fs::write(path, json).unwrap();
    }

    pub fn is_deleted(&self, segment_id: &SegmentId, docid: DocId) -> bool {
        self.deleted
            .get(&segment_id)
            .map_or(false, |set| set.contains(&docid))
    }

    pub fn is_empty(&self) -> bool {
        self.deleted.is_empty()
    }

    pub fn remove_segments_cloned(&self, segments_to_remove: &HashSet<SegmentId>) -> Self {
        let deleted: HashMap<_, _> = self
            .deleted
            .iter()
            .filter(|(segment_id, _)| !segments_to_remove.contains(segment_id))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Self { deleted }
    }

    pub fn merge(&self, other: &Self) -> Self {
        let mut deleted = self.deleted.clone();
        for (seg, docids) in &other.deleted {
            deleted
                .entry(seg.clone())
                .or_insert(HashSet::new())
                .extend(docids);
        }
        Self { deleted }
    }
}
