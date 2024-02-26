use std::{collections::HashMap, sync::Arc};

use tantivy_common::TerminatingWrite;

use crate::{index::IndexMerger, Directory, DocId};

use super::{PrimaryKeyDictBuilder, PrimaryKeyPersistentSegmentData};

#[derive(Default)]
pub struct PrimaryKeyMerger {}

impl IndexMerger for PrimaryKeyMerger {
    fn merge(
        &self,
        directory: &dyn Directory,
        index_directory: &std::path::Path,
        index: &crate::schema::Index,
        segments: &[&Arc<dyn crate::index::IndexSegmentData>],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        let mut keys = HashMap::<Vec<u8>, DocId>::new();
        for (&segment, segment_docid_mapping) in segments.iter().zip(docid_mappings.iter()) {
            let segment_data = segment
                .downcast_ref::<PrimaryKeyPersistentSegmentData>()
                .unwrap();
            let primary_key_dict = &segment_data.keys;
            for (key, docid) in primary_key_dict.iter() {
                if let Some(docid) = segment_docid_mapping[docid as usize] {
                    keys.insert(key.clone(), docid);
                }
            }
        }
        let mut keys: Vec<_> = keys.iter().collect();
        keys.sort_by(|a, b| a.0.cmp(b.0));

        let index_path = index_directory.join(index.name());
        let writer = directory.open_write(&index_path).unwrap();
        let mut primary_key_dict_writer = PrimaryKeyDictBuilder::new(writer);
        for (key, docid) in keys.iter() {
            primary_key_dict_writer.insert(key, docid).unwrap();
        }
        primary_key_dict_writer
            .finish()
            .unwrap()
            .terminate()
            .unwrap();
    }
}
