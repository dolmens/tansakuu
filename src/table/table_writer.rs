use std::sync::Arc;

use crate::{
    document::{value_to_string, InputDocument, Value},
    query::Term,
    util::hash::hash_string_64,
};

use super::{
    segment::{BuildingSegmentData, SegmentWriter},
    Table, TableReader,
};

pub struct TableWriter<'a> {
    segment_writer: SegmentWriter,
    table_reader: Arc<TableReader>,
    table: &'a Table,
}

impl<'a> TableWriter<'a> {
    pub fn new(table: &'a Table) -> Self {
        let segment_writer = SegmentWriter::new(table.schema());
        table.add_building_segment(segment_writer.building_segment().clone());
        let table_reader = table.reader();

        Self {
            segment_writer,
            table_reader,
            table,
        }
    }

    pub fn add_doc(&mut self, doc: &InputDocument) {
        self.segment_writer.add_doc(doc);
    }

    pub fn delete_doc(&mut self, term: &Term) {
        let hashkey = hash_string_64(term.keyword());
        self.table_reader
            .primary_key_index_reader()
            .and_then(|reader| reader.get_by_hashkey(hashkey))
            .and_then(|docid| self.table_reader.segment_of_docid(docid))
            .map(|(segment_id, docid)| self.segment_writer.delete_doc(segment_id.clone(), docid));
    }

    pub fn new_segment(&mut self) {
        self.segment_writer = SegmentWriter::new(self.table.schema());
        let new_segment = self.building_segment().clone();
        self.table.add_building_segment(new_segment);
        self.table_reader = self.table.reader();
    }

    pub fn building_segment(&self) -> &Arc<BuildingSegmentData> {
        self.segment_writer.building_segment()
    }
}
