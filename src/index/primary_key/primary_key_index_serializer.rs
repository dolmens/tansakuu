use std::sync::Arc;

use crate::{index::IndexSerializer, schema::Index};

use super::{PrimaryKeyIndexBuildingSegmentData, PrimaryKeyIndexSerializerWriter};

pub struct PrimaryKeyIndexSerializer {
    index_name: String,
    index_data: Arc<PrimaryKeyIndexBuildingSegmentData>,
}

impl PrimaryKeyIndexSerializer {
    pub fn new(index: &Index, index_data: Arc<PrimaryKeyIndexBuildingSegmentData>) -> Self {
        Self {
            index_name: index.name().to_string(),
            index_data,
        }
    }
}

impl IndexSerializer for PrimaryKeyIndexSerializer {
    fn serialize(&self, directory: &std::path::Path) {
        let path = directory.join(&self.index_name);
        let mut writer = PrimaryKeyIndexSerializerWriter::new(path);
        let keys = self.index_data.keys();
        for (key, &docid) in &keys {
            writer.write(key, docid);
        }
    }
}
