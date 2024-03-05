use std::collections::HashMap;

use crate::{
    columnar::{ColumnWriter, ColumnWriterFactory},
    document::InnerInputDocument,
    schema::SchemaRef,
    DocId,
};

use super::BuildingSegmentColumnData;

pub struct SegmentColumnWriter {
    columns: Vec<Box<dyn ColumnWriter>>,
}

impl SegmentColumnWriter {
    pub fn new(schema: &SchemaRef) -> Self {
        let column_writer_factory = ColumnWriterFactory::default();
        let columns = schema
            .columns()
            .iter()
            .map(|field| column_writer_factory.create(field))
            .collect();

        Self { columns }
    }

    pub fn add_document(&mut self, document: &InnerInputDocument, _docid: DocId) {
        for writer in &mut self.columns {
            if let Some(value) = document.get_field(writer.field().name()) {
                writer.add_value(Some(value));
            } else {
                writer.add_value(None);
            }
        }
    }

    pub fn column_data(&self) -> BuildingSegmentColumnData {
        let mut columns = HashMap::new();
        for writer in &self.columns {
            columns.insert(writer.field().name().to_string(), writer.column_data());
        }

        BuildingSegmentColumnData::new(columns)
    }
}
