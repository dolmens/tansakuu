use std::collections::HashMap;

use crate::{
    column::{ColumnWriter, ColumnWriterFactory},
    document::Document,
    schema::SchemaRef,
    DocId,
};

pub struct TableColumnWriter {
    columns: HashMap<String, Box<dyn ColumnWriter>>,
}

impl TableColumnWriter {
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

    pub fn column_writers(&self) -> impl Iterator<Item = (&str, &dyn ColumnWriter)> {
        self.columns
            .iter()
            .map(|(name, writer)| (name.as_str(), writer.as_ref()))
    }
}
