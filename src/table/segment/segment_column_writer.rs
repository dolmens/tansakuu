use std::collections::HashMap;

use crate::{
    column::{ColumnWriter, ColumnWriterFactory},
    document::Document,
    schema::SchemaRef,
    DocId,
};

use super::BuildingSegmentColumnData;

pub struct SegmentColumnWriter {
    columns: HashMap<String, Box<dyn ColumnWriter>>,
}

impl SegmentColumnWriter {
    pub fn new(schema: &SchemaRef) -> Self {
        let mut columns: HashMap<String, Box<dyn ColumnWriter>> = HashMap::new();
        let column_writer_factory = ColumnWriterFactory::new();
        for column in schema.columns() {
            let column_writer = column_writer_factory.create(column);
            columns.insert(column.name().to_string(), column_writer);
        }

        Self { columns }
    }

    pub fn add_doc(&mut self, doc: &Document, _docid: DocId) {
        for (name, value) in doc.fields() {
            if let Some(writer) = self.columns.get_mut(name) {
                writer.add_doc(value);
            }
        }
    }

    pub fn column_data(&self) -> BuildingSegmentColumnData {
        let mut columns = HashMap::new();
        for (name, writer) in &self.columns {
            columns.insert(name.to_string(), writer.column_data());
        }

        BuildingSegmentColumnData::new(columns)
    }
}