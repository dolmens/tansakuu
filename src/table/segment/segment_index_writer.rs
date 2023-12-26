use std::collections::{BTreeSet, HashMap};

use crate::{
    document::Document,
    index::{IndexWriter, IndexWriterFactory},
    schema::SchemaRef,
    DocId,
};

use super::BuildingSegmentIndexData;

pub struct SegmentIndexWriter {
    indexes: HashMap<String, Box<dyn IndexWriter>>,
    schema: SchemaRef,
}

impl SegmentIndexWriter {
    pub fn new(schema: &SchemaRef) -> Self {
        let mut indexes: HashMap<String, Box<dyn IndexWriter>> = HashMap::new();
        let index_writer_factory = IndexWriterFactory::default();
        for index in schema.indexes() {
            let index_writer = index_writer_factory.create(index);
            indexes.insert(index.name().to_string(), index_writer);
        }

        Self {
            indexes,
            schema: schema.clone(),
        }
    }

    pub fn add_doc(&mut self, doc: &Document, docid: DocId) {
        let mut indexes = BTreeSet::new();
        for (field, value) in doc.fields() {
            if let Some(field) = self.schema.field(field) {
                for index in self.schema.indexes_of_field(field) {
                    let index_writer = self.indexes.get_mut(index.name()).unwrap();
                    index_writer.add_field(field.name(), value);
                    indexes.insert(index.name());
                }
            }
        }

        for index in indexes {
            let index_writer = self.indexes.get_mut(index).unwrap();
            index_writer.end_document(docid);
        }
    }

    pub fn index_data(&self) -> BuildingSegmentIndexData {
        let mut indexes = HashMap::new();
        for (name, writer) in &self.indexes {
            indexes.insert(name.to_string(), writer.index_data());
        }

        BuildingSegmentIndexData::new(indexes)
    }
}
