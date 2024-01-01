use crate::{table::SegmentId, DocId};

use serde::Serialize;

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Mutex,
};

#[derive(Default, Debug, Serialize)]
pub struct BuildingDeletionMap {
    deleted: Mutex<HashMap<SegmentId, HashSet<DocId>>>,
}

impl BuildingDeletionMap {
    pub fn is_deleted(&self, segment_id: SegmentId, docid: DocId) -> bool {
        self.deleted
            .lock()
            .unwrap()
            .get(&segment_id)
            .map_or(false, |set| set.contains(&docid))
    }

    pub fn delete_doc(&self, segment_id: SegmentId, docid: DocId) {
        self.deleted
            .lock()
            .unwrap()
            .entry(segment_id)
            .or_insert(HashSet::new())
            .insert(docid);
    }

    pub fn is_empty(&self) -> bool {
        self.deleted.lock().unwrap().is_empty()
    }

    pub fn save(&self, path: impl AsRef<Path>) {
        let json = serde_json::to_string_pretty(self).unwrap();
        fs::write(path, json).unwrap();
    }
}
