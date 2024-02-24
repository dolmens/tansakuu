use std::sync::Arc;

use tantivy_common::TerminatingWrite;

use crate::{
    deletionmap::BuildingDeletionMap, index::IndexSerializer, schema::IndexRef, Directory,
};

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
        index_directory: &std::path::Path,
        deletionmap: &BuildingDeletionMap,
    ) {
        let mut keys: Vec<_> = self
            .index_data
            .keys
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        keys.sort_by(|a, b| a.0.to_be_bytes().cmp(&b.0.to_be_bytes()));
        let index_path = index_directory.join(&self.index_name);
        let index_writer = directory.open_write(&index_path).unwrap();
        let mut primary_key_dict_writer = PrimaryKeyDictBuilder::new(index_writer);
        for (key, docid) in keys.iter() {
            if !deletionmap.is_deleted(*docid) {
                primary_key_dict_writer
                    .insert(key.to_be_bytes(), docid)
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
