use std::sync::Arc;

use crate::{index::IndexSerializer, schema::Index};

use super::{UniqueKeyIndexBuildingSegmentData, UniqueKeyIndexSerializerWriter};

pub struct UniqueKeyIndexSerializer {
    index_name: String,
    index_data: Arc<UniqueKeyIndexBuildingSegmentData>,
}

impl UniqueKeyIndexSerializer {
    pub fn new(index: &Index, index_data: Arc<UniqueKeyIndexBuildingSegmentData>) -> Self {
        Self {
            index_name: index.name().to_string(),
            index_data,
        }
    }
}

impl IndexSerializer for UniqueKeyIndexSerializer {
    fn serialize(&self, directory: &std::path::Path) {
        let path = directory.join(&self.index_name);
        let mut writer = UniqueKeyIndexSerializerWriter::new(path);
        let keys = self.index_data.keys();
        for (key, &docid) in &keys {
            writer.write(key, docid);
        }
    }
}
