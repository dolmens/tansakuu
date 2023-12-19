use std::collections::{BTreeSet, HashMap};

use crate::{
    document::Document,
    index::{IndexWriter, IndexWriterFactory},
    DocId,
};

use super::{segment::BuildingSegment, table::Table};

pub struct TableWriter<'a> {
    docid: DocId,
    indexes: HashMap<String, Box<dyn IndexWriter>>,
    table: &'a Table,
}

impl<'a> TableWriter<'a> {
    pub fn new(table: &'a Table) -> Self {
        let mut indexes = HashMap::new();
        let mut building_segment = BuildingSegment::new();
        let index_writer_factory = IndexWriterFactory::new();
        let schema = table.schema();
        for index in schema.indexes() {
            let index_writer = index_writer_factory.create(index);
            building_segment.add_index_data(index.name().to_string(), index_writer.index_data());
            indexes.insert(index.name().to_string(), index_writer);
        }
        table.add_building_segment(building_segment);

        Self {
            docid: 0,
            indexes,
            table,
        }
    }

    pub fn add_doc(&mut self, doc: &Document) {
        let schema = self.table.schema();
        let mut indexes = BTreeSet::new();
        for (field, value) in doc.fields() {
            for index in schema.indexes_of_field(field) {
                let index_writer = self.indexes.get_mut(index.name()).unwrap();
                index_writer.add_field(field, value);
                indexes.insert(index.name());
            }
        }

        for index in indexes {
            let index_writer = self.indexes.get_mut(index).unwrap();
            index_writer.end_document(self.docid);
        }

        self.docid += 1;
    }

    // pub fn new_segment(&mut self) {

    // }
}

impl<'a> Drop for TableWriter<'a> {
    fn drop(&mut self) {}
}
