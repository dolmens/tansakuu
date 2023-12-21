use std::{fs::File, io::Write, sync::Arc};

use crate::{index::index_serializer::IndexSerializer, schema::Index};

use super::UniqueKeyIndexBuildingSegmentData;

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
        let mut file = File::create(path).unwrap();
        let keys = self.index_data.keys();
        for (key, docid) in &keys {
            writeln!(file, "{} {}", docid, key.clone()).unwrap();
        }
    }
}
