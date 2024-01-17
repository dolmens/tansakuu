use tantivy_common::file_slice::{FileSlice, WrapFile};

use crate::{table::SegmentId, DocId};

use std::{
    collections::{BTreeSet, HashSet},
    fs::File,
    path::Path,
    sync::Arc,
};

use super::{DeletionDict, DeletionDictBuilder};

pub struct DeletionMap {
    dict: DeletionDict,
}

impl DeletionMap {
    pub fn load(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        let file = File::open(path).unwrap();
        let data = FileSlice::new(Arc::new(WrapFile::new(file).unwrap()));
        let dict = DeletionDict::open(data).unwrap();

        Self { dict }
    }

    pub fn save(&self, path: impl AsRef<Path>) {
        let file = File::create(path).unwrap();
        let mut dict_builder = DeletionDictBuilder::new(file);
        for item in self.dict.iter() {
            dict_builder.insert(item).unwrap();
        }
        dict_builder.finish().unwrap();
    }

    pub fn is_deleted(&self, segment_id: &SegmentId, docid: DocId) -> bool {
        let mut keybuf = [0_u8; 36];
        keybuf[..32].copy_from_slice(segment_id.as_bytes());
        keybuf[32..36].copy_from_slice(&docid.to_be_bytes());
        self.dict.contains(keybuf).unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    pub fn remove_segments_cloned(&self, segments_to_remove: &HashSet<SegmentId>) -> Self {
        let mut dict_builder = DeletionDictBuilder::new(Vec::new());
        for item in self.dict.iter() {
            let segment_id = String::from_utf8_lossy(&item[..32]);
            if !segments_to_remove.contains(segment_id.as_ref()) {
                dict_builder.insert(item).unwrap();
            }
        }
        let buf = dict_builder.finish().unwrap();
        let dict = DeletionDict::open(buf.into()).unwrap();

        Self { dict }
    }

    pub fn merge(segments: &[&Self]) -> Self {
        let mut keys = BTreeSet::new();
        for &seg in segments {
            for item in seg.dict.iter() {
                keys.insert(item);
            }
        }
        let mut dict_builder = DeletionDictBuilder::new(Vec::new());
        for item in keys.into_iter() {
            dict_builder.insert(item).unwrap();
        }
        let buf = dict_builder.finish().unwrap();
        let dict = DeletionDict::open(buf.into()).unwrap();

        Self { dict }
    }
}
