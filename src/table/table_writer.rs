use crate::{deletionmap::DeletionMapWriter, document::Document, query::Term, DocId, END_DOCID};

use super::{segment::SegmentWriter, Table};

pub struct TableWriter {
    segment_writer: SegmentWriter,
    deletionmap_writer: DeletionMapWriter,
    table: Table,
}

impl TableWriter {
    pub fn new(table: &Table) -> Self {
        let mut table_data = table.data().lock().unwrap();
        let recent_segment_stat = table_data.recent_segment_stat();
        let segment_writer = SegmentWriter::new(table.schema(), recent_segment_stat);
        table_data.add_building_segment(segment_writer.building_segment_data().clone());
        let deletionmap_writer = DeletionMapWriter::new(&mut table_data);
        table.reinit_reader(table_data.clone());

        Self {
            segment_writer,
            deletionmap_writer,
            table: table.clone(),
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
            if docid < self.deletionmap_writer.doc_count() as DocId {
                self.deletionmap_writer.delete_document(docid);
            } else {
                self.segment_writer
                    .delete_document(docid - self.deletionmap_writer.doc_count() as DocId);
            }
            docid = docid + 1;
        }
    }

    pub fn new_segment(&mut self) {
        let mut table_data = self.table.data().lock().unwrap();
        let directory = table_data.directory();
        self.deletionmap_writer.save(directory);
        table_data.dump_building_segment(self.segment_writer.building_segment_data().clone());
        let recent_segment_stat = table_data.recent_segment_stat();
        self.segment_writer = SegmentWriter::new(self.table.schema(), recent_segment_stat);
        let new_segment = self.segment_writer.building_segment_data().clone();
        table_data.add_building_segment(new_segment);
        self.deletionmap_writer = DeletionMapWriter::new(&mut table_data);
        self.table.reinit_reader(table_data.clone());
    }
}
