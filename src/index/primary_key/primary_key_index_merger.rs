use std::collections::HashMap;

use crate::{index::IndexMerger, DocId};

use super::{PrimaryKeyIndexPersistentSegmentData, PrimaryKeyIndexSerializerWriter};

#[derive(Default)]
pub struct PrimaryKeyIndexMerger {}

impl IndexMerger for PrimaryKeyIndexMerger {
    fn merge(
        &self,
        directory: &std::path::Path,
        index: &crate::schema::Index,
        segments: &[&dyn crate::index::IndexSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        let path = directory.join(index.name());
        let mut writer = PrimaryKeyIndexSerializerWriter::new(path);
        let mut keys = HashMap::<String, DocId>::new();
        for (&segment, segment_docid_mappings) in segments.iter().zip(docid_mappings.iter()) {
            let segment_data = segment
                .downcast_ref::<PrimaryKeyIndexPersistentSegmentData>()
                .unwrap();
            for (key, &docid) in segment_data.keys.iter() {
                if let Some(docid) = segment_docid_mappings[docid as usize] {
                    keys.insert(key.clone(), docid);
                }
            }
        }

        for (key, docid) in keys {
            writer.write(&key, docid);
        }
    }
}
