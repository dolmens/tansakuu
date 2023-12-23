use std::sync::Arc;

use crate::{document::Document, schema::SchemaRef, DocId};

use super::{BuildingSegmentData, SegmentColumnWriter, SegmentIndexWriter};

pub struct SegmentWriter {
    doc_count: usize,
    column_writer: SegmentColumnWriter,
    index_writer: SegmentIndexWriter,
    building_segment_data: Arc<BuildingSegmentData>,
}

impl SegmentWriter {
    pub fn new(schema: &SchemaRef) -> Self {
        let column_writer = SegmentColumnWriter::new(schema);
        let index_writer = SegmentIndexWriter::new(schema);
        let building_segment_data = Arc::new(BuildingSegmentData::new(
            column_writer.column_data(),
            index_writer.index_data(),
        ));

        Self {
            doc_count: 0,
            column_writer,
            index_writer,
            building_segment_data,
        }
    }

    pub fn add_doc(&mut self, doc: &Document, docid: DocId) {
        self.column_writer.add_doc(doc, docid);
        self.index_writer.add_doc(doc, docid);
        self.doc_count += 1;
        self.building_segment_data.set_doc_count(self.doc_count);
    }

    pub fn building_segment_data(&self) -> &Arc<BuildingSegmentData> {
        &self.building_segment_data
    }
}
