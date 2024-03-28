use crate::{
    deletionmap::DeletionMapWriter, document::Document, index::IndexWriterResourceBuilder,
    query::Term, END_DOCID,
};

use super::{segment::SegmentWriter, Table};

pub struct TableWriter {
    segment_writer: SegmentWriter,
    deletionmap_writer: DeletionMapWriter,
    table: Table,
}

impl TableWriter {
    pub fn new(table: &Table) -> Self {
        let mut table_data = table.data().lock().unwrap();
        let tokenizers = table.tokenizers();
        let recent_segment_stat = table_data.recent_segment_stat();
        let index_writer_resource = IndexWriterResourceBuilder::new(tokenizers)
            .set_recent_segment_stat(recent_segment_stat)
            .build();
        let segment_writer = SegmentWriter::new(table.schema(), &index_writer_resource);
        table_data.add_building_segment(segment_writer.building_segment_data().clone());
        let deletionmap_writer = DeletionMapWriter::new(&table_data);
        let deletionmap_reader = deletionmap_writer.reader();
        table_data.set_deletionmap_reader(deletionmap_reader);
        table.reinit_reader(table_data.clone());

        Self {
            segment_writer,
            deletionmap_writer,
            table: table.clone(),
        }
    }

    pub fn add_document<D: Document>(&mut self, document: D) {
        self.segment_writer.add_document(document.into());
    }

    pub fn delete_documents(&mut self, term: &Term) {
        let table_reader = self.table.reader();
        let mut posting_iter = table_reader.index_reader().lookup(term);
        if let Some(posting_iter) = posting_iter.as_deref_mut() {
            let mut docid = 0;
            loop {
                docid = posting_iter.seek(docid).unwrap();
                if docid == END_DOCID {
                    break;
                }
                self.deletionmap_writer.delete_document(docid);
                docid = docid + 1;
            }
        }
    }

    pub fn new_segment(&mut self) {
        let mut table_data = self.table.data().lock().unwrap();
        table_data.dump_building_segment(self.segment_writer.building_segment_data().clone());
        let tokenizers = self.table.tokenizers();
        let recent_segment_stat = table_data.recent_segment_stat();
        let index_writer_resource = IndexWriterResourceBuilder::new(tokenizers)
            .set_recent_segment_stat(recent_segment_stat)
            .build();
        self.segment_writer = SegmentWriter::new(self.table.schema(), &index_writer_resource);
        let new_segment = self.segment_writer.building_segment_data().clone();
        table_data.add_building_segment(new_segment);
        self.deletionmap_writer.reload(&table_data);
        let deletionmap_reader = self.deletionmap_writer.reader();
        table_data.set_deletionmap_reader(deletionmap_reader);
        self.table.reinit_reader(table_data.clone());
    }
}
