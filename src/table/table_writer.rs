use std::sync::Arc;

use crate::document::Document;

use super::{
    segment::{BuildingSegment, SegmentWriter},
    Table,
};

pub struct TableWriter<'a> {
    segment_writer: SegmentWriter,
    table: &'a Table,
}

impl<'a> TableWriter<'a> {
    pub fn new(table: &'a Table) -> Self {
        let segment_writer = SegmentWriter::new(table.schema());
        table.add_building_segment(segment_writer.building_segment().clone());

        Self {
            segment_writer,
            table,
        }
    }

    pub fn add_doc(&mut self, doc: &Document) {
        self.segment_writer.add_doc(doc);
    }

    pub fn new_segment(&mut self) {
        let building_segment = self.building_segment().clone();
        self.segment_writer = SegmentWriter::new(self.table.schema());
        let new_segment = self.building_segment().clone();
        self.table
            .dump_and_add_building_segment(building_segment, new_segment);
    }

    pub fn building_segment(&self) -> &Arc<BuildingSegment> {
        self.segment_writer.building_segment()
    }
}

impl<'a> Drop for TableWriter<'a> {
    fn drop(&mut self) {
        if !self.segment_writer.is_empty() {
            self.table
                .dump_building_segment(self.building_segment().clone());
        }
    }
}
