use crate::{document::Document, schema::SchemaRef, DocId};

use super::{BuildingSegmentData, SegmentColumnWriter, SegmentIndexWriter};

pub struct SegmentWriter {
    column_writer: SegmentColumnWriter,
    index_writer: SegmentIndexWriter,
}

impl SegmentWriter {
    pub fn new(schema: &SchemaRef) -> Self {
        let column_writer = SegmentColumnWriter::new(schema);
        let index_writer = SegmentIndexWriter::new(schema);

        Self {
            column_writer,
            index_writer,
        }
    }

    pub fn add_doc(&mut self, doc: &Document, docid: DocId) {
        self.column_writer.add_doc(doc, docid);
        self.index_writer.add_doc(doc, docid);
    }

    pub fn building_segment_data(&self) -> BuildingSegmentData {
        let column_data = self.column_writer.column_data();
        let index_data = self.index_writer.index_data();

        BuildingSegmentData::new(column_data, index_data)
    }
}
