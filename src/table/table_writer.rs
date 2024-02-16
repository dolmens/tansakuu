use std::sync::Arc;

use crate::{document::Document, query::Term, util::hash::hash_string_64};

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
        let recent_segment_stat = table.recent_segment_stat();
        let segment_writer = SegmentWriter::new(table.schema(), recent_segment_stat.as_ref());
        table.add_building_segment(segment_writer.building_segment().clone());
        let table_reader = table.reader();

        Self {
            segment_writer,
            table_reader,
            table,
        }
    }

    pub fn add_doc<D: Document>(&mut self, doc: &D) {
        self.segment_writer.add_doc(doc);
    }

    pub fn delete_docs(&mut self, term: &Term) {
        let hashkey = hash_string_64(term.keyword());
        self.table_reader
            .primary_key_index_reader()
            .and_then(|reader| reader.get_by_hashkey(hashkey))
            .and_then(|docid| self.table_reader.segment_of_docid(docid))
            .map(|(segment_id, docid)| self.segment_writer.delete_doc(segment_id.clone(), docid));
    }

    pub fn new_segment(&mut self) {
        let recent_segment_stat = self.table.recent_segment_stat();
        self.segment_writer = SegmentWriter::new(self.table.schema(), recent_segment_stat.as_ref());
        let new_segment = self.building_segment().clone();
        self.table.add_building_segment(new_segment);
        self.table_reader = self.table.reader();
    }

    pub fn building_segment(&self) -> &Arc<BuildingSegmentData> {
        self.segment_writer.building_segment()
    }
}
