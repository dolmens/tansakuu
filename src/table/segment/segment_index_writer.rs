use std::collections::{BTreeSet, HashMap};

use crate::{
    document::InnerInputDocument,
    index::{IndexWriter, IndexWriterFactory, IndexWriterResource},
    schema::SchemaRef,
    DocId,
};

use super::BuildingSegmentIndexData;

pub struct SegmentIndexWriter {
    indexes: HashMap<String, Box<dyn IndexWriter>>,
    schema: SchemaRef,
}

impl SegmentIndexWriter {
    pub fn new(schema: &SchemaRef, writer_resource: &IndexWriterResource) -> Self {
        let mut indexes: HashMap<String, Box<dyn IndexWriter>> = HashMap::new();
        let index_writer_factory = IndexWriterFactory::default();
        for index in schema.indexes() {
            let index_writer = index_writer_factory.create(index, writer_resource);
            indexes.insert(index.name().to_string(), index_writer);
        }

        Self {
            indexes,
            schema: schema.clone(),
        }
    }

    pub fn add_document(&mut self, document: &InnerInputDocument, docid: DocId) {
        let mut indexes = BTreeSet::new();
        for (field, value) in document.iter_fields_and_values() {
            if let Some((field, field_indexes)) = self.schema.field(field) {
                for index in field_indexes.iter() {
                    let index_writer = self.indexes.get_mut(index.name()).unwrap();
                    index_writer.add_field(field, value);
                    indexes.insert(index.name().to_string());
                }
            }
        }

        for index in indexes {
            let index_writer = self.indexes.get_mut(&index).unwrap();
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
