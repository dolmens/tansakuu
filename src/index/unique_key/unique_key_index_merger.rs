use std::{collections::HashMap, fs::File, io::Write};

use crate::{index::IndexMerger, DocId};

use super::UniqueKeyIndexSegmentData;

#[derive(Default)]
pub struct UniqueKeyIndexMerger {}

impl IndexMerger for UniqueKeyIndexMerger {
    fn merge(
        &self,
        directory: &std::path::Path,
        index: &crate::schema::Index,
        segments: &[&dyn crate::index::IndexSegmentData],
        _doc_counts: &[usize],
    ) {
        let path = directory.join(index.name());
        let mut file = File::create(path).unwrap();
        let mut keys = HashMap::<String, DocId>::new();
        for &segment in segments {
            let segment_data = segment.downcast_ref::<UniqueKeyIndexSegmentData>().unwrap();
            for (key, &docid) in segment_data.keys.iter() {
                keys.insert(key.clone(), docid);
            }
        }

        for (key, docid) in keys {
            writeln!(file, "{} {}", docid, key).unwrap();
        }
    }
}
