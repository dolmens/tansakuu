use std::sync::Arc;

use crate::{document::Document, DocId};

use super::{
    segment::{BuildingSegment, SegmentWriter},
    Table,
};

pub struct TableWriter<'a> {
    docid: DocId,
    segment_writer: SegmentWriter,
    table: &'a Table,
}

impl<'a> TableWriter<'a> {
    pub fn new(table: &'a Table) -> Self {
        let segment_writer = SegmentWriter::new(table.schema());
        table.add_building_segment(segment_writer.building_segment().clone());

        Self {
            docid: 0,
            segment_writer,
            table,
        }
    }

    pub fn add_doc(&mut self, doc: &Document) {
        self.segment_writer.add_doc(doc, self.docid);
        self.docid += 1;
    }

    pub fn new_segment(&mut self) {
        self.table.dump_segment(self.building_segment());

        self.docid = 0;
        self.segment_writer = SegmentWriter::new(self.table.schema());
        self.table
            .add_building_segment(self.building_segment().clone());
    }

    pub fn building_segment(&self) -> &Arc<BuildingSegment> {
        self.segment_writer.building_segment()
    }
}

impl<'a> Drop for TableWriter<'a> {
    fn drop(&mut self) {
        if self.docid > 0 {
            self.table.dump_segment(self.building_segment());
        }
    }
}
