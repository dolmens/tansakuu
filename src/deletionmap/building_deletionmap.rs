use tantivy_common::TerminatingWrite;

use crate::{table::SegmentId, util::layered_hashmap::LayeredHashMap, Directory, DocId};

use std::path::Path;

use super::DeletionDictBuilder;

#[derive(Clone)]
pub struct BuildingDeletionMap {
    deleted: LayeredHashMap<SegmentId, LayeredHashMap<DocId, ()>>,
}

impl BuildingDeletionMap {
    pub fn new(deleted: LayeredHashMap<SegmentId, LayeredHashMap<DocId, ()>>) -> Self {
        Self { deleted }
    }

    pub fn is_deleted(&self, segment_id: &SegmentId, docid: DocId) -> bool {
        self.deleted
            .get(segment_id)
            .map_or(false, |seg| seg.contains_key(&docid))
    }

    pub fn is_empty(&self) -> bool {
        self.deleted.is_empty()
    }

    pub fn save(&self, directory: &dyn Directory, path: impl AsRef<Path>) {
        let writer = directory.open_write(path.as_ref()).unwrap();
        let mut dict_builder = DeletionDictBuilder::new(writer);
        let mut keybuf = [0_u8; 36];
        for (seg, docs) in self.deleted.iter() {
            keybuf[..32].copy_from_slice(seg.as_bytes());
            for (&docid, _) in docs.iter() {
                keybuf[32..36].copy_from_slice(&docid.to_be_bytes());
                dict_builder.insert(&keybuf).unwrap();
            }
        }
        dict_builder.finish().unwrap().terminate().unwrap();
    }
}
