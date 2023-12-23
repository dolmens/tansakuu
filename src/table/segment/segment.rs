use std::{collections::HashMap, path::Path, sync::Arc};

use crate::{
    column::{ColumnSegmentData, ColumnSegmentDataFactory},
    index::{IndexSegmentData, IndexSegmentDataFactory},
    schema::SchemaRef,
};

pub struct Segment {
    segment_name: String,
    indexes: HashMap<String, Arc<dyn IndexSegmentData>>,
    columns: HashMap<String, Arc<dyn ColumnSegmentData>>,
}

impl Segment {
    pub fn new(segment_name: String,schema: &SchemaRef, directory: impl AsRef<Path>) -> Self {
        let mut indexes = HashMap::new();
        let index_segment_data_factory = IndexSegmentDataFactory::new();
        let index_directory = directory.as_ref().join("index");
        for index in schema.indexes() {
            let index_segment_data_builder = index_segment_data_factory.create_builder(index);
            let index_path = index_directory.join(index.name());
            let index_segment_data = index_segment_data_builder.build(index, &index_path);
            indexes.insert(index.name().to_string(), index_segment_data.into());
        }

        let mut columns = HashMap::new();
        let column_segment_data_factory = ColumnSegmentDataFactory::new();
        let column_directory = directory.as_ref().join("column");
        for field in schema.columns() {
            let column_segment_data_builder = column_segment_data_factory.create_builder(field);
            let column_path = column_directory.join(field.name());
            let column_segment_data = column_segment_data_builder.build(field, &column_path);
            columns.insert(field.name().to_string(), column_segment_data.into());
        }

        Self { segment_name, indexes, columns }
    }

    pub fn segment_name(&self) -> &str {
        &self.segment_name
    }

    pub fn index_data(&self, index: &str) -> &Arc<dyn IndexSegmentData> {
        self.indexes.get(index).unwrap()
    }

    pub fn column_data(&self, column: &str) -> &Arc<dyn ColumnSegmentData> {
        self.columns.get(column).unwrap()
    }
}
