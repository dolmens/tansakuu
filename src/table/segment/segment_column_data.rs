use std::{collections::HashMap, path::Path, sync::Arc};

use crate::{
    column::{ColumnSegmentData, ColumnSegmentDataFactory},
    schema::SchemaRef,
};

pub struct SegmentColumnData {
    columns: HashMap<String, Arc<dyn ColumnSegmentData>>,
}

impl SegmentColumnData {
    pub fn open(directory: impl AsRef<Path>, schema: &SchemaRef) -> Self {
        let directory = directory.as_ref();
        let mut columns = HashMap::new();
        let column_segment_data_factory = ColumnSegmentDataFactory::default();
        for field in schema.columns() {
            let column_segment_data_builder = column_segment_data_factory.create_builder(field);
            let column_path = directory.join(field.name());
            let column_segment_data = column_segment_data_builder.build(field, &column_path);
            columns.insert(field.name().to_string(), column_segment_data.into());
        }

        Self { columns }
    }

    pub fn new(columns: HashMap<String, Arc<dyn ColumnSegmentData>>) -> Self {
        Self { columns }
    }

    pub fn column(&self, name: &str) -> Option<&Arc<dyn ColumnSegmentData>> {
        self.columns.get(name)
    }
}
