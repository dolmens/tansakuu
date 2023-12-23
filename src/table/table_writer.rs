use std::sync::Arc;

use crate::{document::Document, DocId};

use super::{
    segment::{BuildingSegment, SegmentWriter},
    Table,
};

pub struct TableWriter<'a> {
    docid: DocId,
    segment_writer: SegmentWriter,
    building_segment: Arc<BuildingSegment>,
    table: &'a Table,
}

impl<'a> TableWriter<'a> {
    pub fn new(table: &'a Table) -> Self {
        let segment_writer = SegmentWriter::new(table.schema());
        let segment_data = segment_writer.building_segment_data().clone();
        let building_segment = Arc::new(BuildingSegment::new(segment_data));
        table.add_building_segment(building_segment.clone());

        Self {
            docid: 0,
            segment_writer,
            building_segment,
            table,
        }
    }

    pub fn add_doc(&mut self, doc: &Document) {
        self.segment_writer.add_doc(doc, self.docid);
        self.docid += 1;
    }

    pub fn new_segment(&mut self) {
        self.table.dump_segment(self.building_segment.clone());

        self.segment_writer = SegmentWriter::new(self.table.schema());
        let segment_data = self.segment_writer.building_segment_data().clone();
        let building_segment = Arc::new(BuildingSegment::new(segment_data));
        self.table.add_building_segment(building_segment.clone());
    }
}

impl<'a> Drop for TableWriter<'a> {
    fn drop(&mut self) {
        // dump
    }
}
