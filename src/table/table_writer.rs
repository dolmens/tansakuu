use std::sync::Arc;

use crate::{deletionmap::DeletionMapWriter, document::Document, query::Term, DocId, END_DOCID};

use super::{
    segment::{BuildingSegmentData, SegmentWriter},
    Table,
};

pub struct TableWriter<'a> {
    fixed_doc_count: usize,
    segment_writer: SegmentWriter,
    deletionmap_writer: DeletionMapWriter,
    table: &'a Table,
}

impl<'a> TableWriter<'a> {
    pub fn new(table: &'a Table) -> Self {
        let recent_segment_stat = table.recent_segment_stat();
        let segment_writer = SegmentWriter::new(table.schema(), recent_segment_stat.as_ref());
        table.add_building_segment(segment_writer.building_segment().clone());
        let mut table_data = table.data().lock().unwrap();
        let fixed_doc_count = table_data.fixed_doc_count();
        let deletionmap_writer = DeletionMapWriter::new(&mut table_data);
        drop(table_data);

        Self {
            fixed_doc_count,
            segment_writer,
            deletionmap_writer,
            table,
        }
    }

    pub fn add_document<D: Document>(&mut self, doc: &D) {
        self.segment_writer.add_document(doc);
    }

    pub fn delete_documents(&mut self, term: &Term) {
        let table_reader = self.table.reader();
        let mut posting_iter = table_reader.index_reader().lookup(term).unwrap();
        let mut docid = 0;
        loop {
            docid = posting_iter.seek(docid).unwrap();
            if docid == END_DOCID {
                break;
            }
            if docid < self.fixed_doc_count as DocId {
                self.deletionmap_writer.delete_document(docid);
            } else {
                self.segment_writer
                    .delete_document(docid - self.fixed_doc_count as DocId);
            }
            docid = docid + 1;
        }
    }

    pub fn new_segment(&mut self) {
        // TODO; dump current building segment and add new building segment
        // TODO: lock table_data first, then do everything
        let table_data = self.table.data().lock().unwrap();
        let directory = table_data.directory();
        self.deletionmap_writer.save(directory);
        drop(table_data);
        let recent_segment_stat = self.table.recent_segment_stat();
        self.segment_writer = SegmentWriter::new(self.table.schema(), recent_segment_stat.as_ref());
        let new_segment = self.building_segment().clone();
        self.table.add_building_segment(new_segment);
        let mut table_data = self.table.data().lock().unwrap();
        self.deletionmap_writer = DeletionMapWriter::new(&mut table_data);
        // TODO: so many reinit readers
        self.table.reinit_reader(table_data.clone());
    }

    pub fn building_segment(&self) -> &Arc<BuildingSegmentData> {
        self.segment_writer.building_segment()
    }
}
