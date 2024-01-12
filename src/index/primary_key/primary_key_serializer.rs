use std::sync::Arc;

use crate::{index::IndexSerializer, schema::Index};

use super::{PrimaryKeyBuildingSegmentData, PrimaryKeySerializerWriter};

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
        let path = directory.join(&self.index_name);
        let mut writer = PrimaryKeySerializerWriter::new(path);
        let keys = self.index_data.keys();
        for (key, &docid) in &keys {
            writer.write(key, docid);
        }
    }
}
