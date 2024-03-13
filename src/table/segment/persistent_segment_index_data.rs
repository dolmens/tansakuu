use std::{collections::HashMap, path::Path, sync::Arc};

use crate::{
    index::{IndexSegmentData, IndexSegmentDataFactory},
    schema::SchemaRef,
    Directory,
};

pub struct PersistentSegmentIndexData {
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
}

impl PersistentSegmentIndexData {
    pub fn open(
        directory: &dyn Directory,
        index_path: impl AsRef<Path>,
        schema: &SchemaRef,
    ) -> Self {
        let index_path = index_path.as_ref();
        let mut indexes = HashMap::new();
        let index_segment_data_factory = IndexSegmentDataFactory::default();
        for index in schema.indexes() {
            let index_segment_data_builder = index_segment_data_factory.create_builder(index);
            let index_segment_data = index_segment_data_builder.build(index, directory, index_path);
            indexes.insert(index.name().to_string(), index_segment_data.into());
        }

        Self { indexes }
    }

    pub fn index(&self, name: &str) -> Option<&Arc<dyn IndexSegmentData>> {
        self.indexes.get(name)
    }
}
