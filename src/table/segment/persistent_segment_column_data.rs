use std::{collections::HashMap, path::Path, sync::Arc};

use crate::{
    column::{ColumnSegmentData, ColumnSegmentDataFactory},
    schema::SchemaRef,
    Directory,
};

pub struct PersistentSegmentColumnData {
    columns: HashMap<String, Arc<dyn ColumnSegmentData>>,
}

impl PersistentSegmentColumnData {
    pub fn open(
        directory: &dyn Directory,
        column_directory: impl AsRef<Path>,
        schema: &SchemaRef,
    ) -> Self {
        let column_directory = column_directory.as_ref();
        let mut columns = HashMap::new();
        let column_segment_data_factory = ColumnSegmentDataFactory::default();
        for field in schema.columns() {
            let column_segment_data_builder = column_segment_data_factory.create_builder(field);
            let column_path = column_directory.join(field.name());
            let column_segment_data =
                column_segment_data_builder.build(field, directory, &column_path);
            columns.insert(field.name().to_string(), column_segment_data.into());
        }

        Self { columns }
    }

    pub fn column(&self, name: &str) -> Option<&Arc<dyn ColumnSegmentData>> {
        self.columns.get(name)
    }
}
