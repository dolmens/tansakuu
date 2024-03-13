use std::sync::Arc;

use tantivy_common::TerminatingWrite;

use crate::{
    index::{IndexSegmentData, IndexSerializer},
    schema::IndexRef,
    Directory, DocId,
};

use super::{UniqueKeyBuildingSegmentData, UniqueKeyDictBuilder};

#[derive(Default)]
pub struct UniqueKeySerializer {}

impl IndexSerializer for UniqueKeySerializer {
    fn serialize(
        &self,
        index: &IndexRef,
        index_data: &Arc<dyn IndexSegmentData>,
        directory: &dyn Directory,
        index_path: &std::path::Path,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    ) {
        let unique_key_index_data = index_data
            .clone()
            .downcast_arc::<UniqueKeyBuildingSegmentData>()
            .ok()
            .unwrap();

        let mut keys: Vec<_> = unique_key_index_data
            .keys
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        keys.sort_by(|a, b| a.0.to_be_bytes().cmp(&b.0.to_be_bytes()));
        let index_path = index_path.join(index.name());
        let index_writer = directory.open_write(&index_path).unwrap();
        let mut primary_key_dict_writer = UniqueKeyDictBuilder::new(index_writer);
        for (key, docid) in keys.iter() {
            if let Some(docid) = if let Some(docid_mapping) = docid_mapping {
                docid_mapping[*docid as usize]
            } else {
                Some(*docid)
            } {
                primary_key_dict_writer
                    .insert(key.to_be_bytes(), &docid)
                    .unwrap();
            }
        }
        primary_key_dict_writer
            .finish()
            .unwrap()
            .terminate()
            .unwrap();
    }
}
