use std::sync::Arc;

use crate::{document::Document, schema::SchemaRef, DocId};

use super::{BuildingSegment, BuildingSegmentData, SegmentColumnWriter, SegmentIndexWriter};

pub struct SegmentWriter {
    doc_count: usize,
    column_writer: SegmentColumnWriter,
    index_writer: SegmentIndexWriter,
    building_segment: Arc<BuildingSegment>,
}

impl SegmentWriter {
    pub fn new(schema: &SchemaRef) -> Self {
        let column_writer = SegmentColumnWriter::new(schema);
        let index_writer = SegmentIndexWriter::new(schema);
        let building_segment_data =
            BuildingSegmentData::new(column_writer.column_data(), index_writer.index_data());
        let building_segment = Arc::new(BuildingSegment::new(building_segment_data));

        Self {
            doc_count: 0,
            column_writer,
            index_writer,
            building_segment,
        }
    }

    pub fn add_doc(&mut self, doc: &Document, docid: DocId) {
        self.column_writer.add_doc(doc, docid);
        self.index_writer.add_doc(doc, docid);
        self.doc_count += 1;
        self.building_segment
            .segment_data()
            .set_doc_count(self.doc_count);
    }

    pub fn building_segment(&self) -> &Arc<BuildingSegment> {
        &self.building_segment
    }
}
