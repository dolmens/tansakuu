use std::{fs::File, sync::Arc};

use crate::{index::IndexSerializer, schema::Index};

use super::{PrimaryKeyBuildingSegmentData, PrimaryKeyDictBuilder};

pub struct PrimaryKeySerializer {
    index_name: String,
    index_data: Arc<PrimaryKeyBuildingSegmentData>,
}

impl PrimaryKeySerializer {
    pub fn new(index: &Index, index_data: Arc<PrimaryKeyBuildingSegmentData>) -> Self {
        Self {
            index_name: index.name().to_string(),
            index_data,
        }
    }
}

impl IndexSerializer for PrimaryKeySerializer {
    fn serialize(&self, directory: &std::path::Path) {
        let mut keys: Vec<_> = self
            .index_data
            .keys
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        keys.sort_by(|a, b| a.0.to_be_bytes().cmp(&b.0.to_be_bytes()));
        let index_path = directory.join(&self.index_name);
        let index_file = File::create(index_path).unwrap();
        let mut primary_key_dict_writer = PrimaryKeyDictBuilder::new(index_file);
        for (key, docid) in keys.iter() {
            primary_key_dict_writer
                .insert(key.to_be_bytes(), docid)
                .unwrap();
        }
        primary_key_dict_writer.finish().unwrap();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_basic() {}
}
