use std::{collections::HashMap, path::Path, sync::Arc};

use crate::{
    column::{ColumnSegmentData, ColumnSegmentDataFactory},
    index::{IndexSegmentData, IndexSegmentDataFactory},
    schema::SchemaRef,
};

use super::SegmentMeta;

pub struct Segment {
    name: String,
    meta: SegmentMeta,
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
    columns: HashMap<String, Arc<dyn ColumnSegmentData>>,
}

impl Segment {
    pub fn new(segment_name: String, schema: &SchemaRef, directory: impl AsRef<Path>) -> Self {
        let directory = directory.as_ref();
        let segment_directory = directory.join(&segment_name);
        let meta = SegmentMeta::load(segment_directory.join("meta.json"));
        let mut indexes = HashMap::new();
        let index_segment_data_factory = IndexSegmentDataFactory::new();
        let index_directory = segment_directory.join("index");
        for index in schema.indexes() {
            let index_segment_data_builder = index_segment_data_factory.create_builder(index);
            let index_path = index_directory.join(index.name());
            let index_segment_data = index_segment_data_builder.build(index, &index_path);
            indexes.insert(index.name().to_string(), index_segment_data.into());
        }

        let mut columns = HashMap::new();
        let column_segment_data_factory = ColumnSegmentDataFactory::new();
        let column_directory = segment_directory.join("column");
        for field in schema.columns() {
            let column_segment_data_builder = column_segment_data_factory.create_builder(field);
            let column_path = column_directory.join(field.name());
            let column_segment_data = column_segment_data_builder.build(field, &column_path);
            columns.insert(field.name().to_string(), column_segment_data.into());
        }

        Self {
            name: segment_name,
            meta,
            indexes,
            columns,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn doc_count(&self) -> usize {
        self.meta.doc_count()
    }

    pub fn index_data(&self, index: &str) -> &Arc<dyn IndexSegmentData> {
        self.indexes.get(index).unwrap()
    }

    pub fn column_data(&self, column: &str) -> &Arc<dyn ColumnSegmentData> {
        self.columns.get(column).unwrap()
    }
}
