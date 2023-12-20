use std::collections::{BTreeSet, HashMap};

use crate::{
    document::Document,
    index::{IndexWriter, IndexWriterFactory},
    schema::SchemaRef,
    DocId,
};

pub struct TableIndexWriter {
    indexes: HashMap<String, Box<dyn IndexWriter>>,
    schema: SchemaRef,
}

impl TableIndexWriter {
    pub fn new(schema: &SchemaRef) -> Self {
        let mut indexes: HashMap<String, Box<dyn IndexWriter>> = HashMap::new();
        let index_writer_factory = IndexWriterFactory::new();
        for index in schema.indexes() {
            let index_writer = index_writer_factory.create(index);
            indexes.insert(index.name().to_string(), index_writer);
        }

        Self { indexes, schema: schema.clone() }
    }

    pub fn add_doc(&mut self, doc: &Document, docid: DocId) {
        let mut indexes = BTreeSet::new();
        for (field, value) in doc.fields() {
            for index in self.schema.indexes_of_field(field) {
                let index_writer = self.indexes.get_mut(index.name()).unwrap();
                index_writer.add_field(field, value);
                indexes.insert(index.name());
            }
        }

        for index in indexes {
            let index_writer = self.indexes.get_mut(index).unwrap();
            index_writer.end_document(docid);
        }
    }

    pub fn index_writers(&self) -> impl Iterator<Item = (&str, &dyn IndexWriter)> {
        self.indexes
            .iter()
            .map(|(name, writer)| (name.as_str(), writer.as_ref()))
    }
}
