use std::sync::Arc;

use tantivy_common::TerminatingWrite;

use crate::{index::IndexSerializer, schema::IndexRef, Directory, DocId};

use super::{PrimaryKeyBuildingSegmentData, PrimaryKeyDictBuilder};

pub struct PrimaryKeySerializer {
    index_name: String,
    index_data: Arc<PrimaryKeyBuildingSegmentData>,
}

impl PrimaryKeySerializer {
    pub fn new(index: &IndexRef, index_data: Arc<PrimaryKeyBuildingSegmentData>) -> Self {
        Self {
            index_name: index.name().to_string(),
            index_data,
        }
    }
}

impl IndexSerializer for PrimaryKeySerializer {
    fn serialize(
        &self,
        directory: &dyn Directory,
        index_path: &std::path::Path,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    ) {
        let mut keys: Vec<_> = self
            .index_data
            .keys
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        keys.sort_by(|a, b| a.0.to_be_bytes().cmp(&b.0.to_be_bytes()));
        let index_path = index_path.join(&self.index_name);
        let index_writer = directory.open_write(&index_path).unwrap();
        let mut primary_key_dict_writer = PrimaryKeyDictBuilder::new(index_writer);
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_basic() {}
}
