use std::{collections::HashMap, fs::File, sync::Arc};

use crate::{index::IndexMerger, DocId};

use super::{PrimaryKeyDictBuilder, PrimaryKeyPersistentSegmentData};

#[derive(Default)]
pub struct PrimaryKeyMerger {}

impl IndexMerger for PrimaryKeyMerger {
    fn merge(
        &self,
        directory: &std::path::Path,
        index: &crate::schema::Index,
        segments: &[&Arc<dyn crate::index::IndexSegmentData>],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        let mut keys = HashMap::<Vec<u8>, DocId>::new();
        for (&segment, segment_docid_mappings) in segments.iter().zip(docid_mappings.iter()) {
            let segment_data = segment
                .downcast_ref::<PrimaryKeyPersistentSegmentData>()
                .unwrap();
            let primary_key_dict = &segment_data.keys;
            for (key, docid) in primary_key_dict.iter() {
                if let Some(docid) = segment_docid_mappings[docid as usize] {
                    keys.insert(key.clone(), docid);
                }
            }
        }
        let mut keys: Vec<_> = keys.iter().collect();
        keys.sort_by(|a, b| a.0.cmp(b.0));

        let index_path = directory.join(index.name());
        let index_file = File::create(index_path).unwrap();
        let mut primary_key_dict_writer = PrimaryKeyDictBuilder::new(index_file);
        for (key, docid) in keys.iter() {
            primary_key_dict_writer.insert(key, docid).unwrap();
        }
        primary_key_dict_writer.finish().unwrap();
    }
}
