use std::sync::Arc;

use crate::{document::InnerInputDocument, index::IndexWriterResource, schema::SchemaRef, DocId};

use super::{BuildingSegmentData, DocCountPublisher, SegmentColumnWriter, SegmentIndexWriter};

pub struct SegmentWriter {
    doc_count: usize,
    doc_count_publisher: DocCountPublisher,
    column_writer: SegmentColumnWriter,
    index_writer: SegmentIndexWriter,
    building_segment_data: Arc<BuildingSegmentData>,
}

impl SegmentWriter {
    pub fn new(schema: &SchemaRef, index_writer_resource: &IndexWriterResource) -> Self {
        let column_writer = SegmentColumnWriter::new(schema);
        let index_writer = SegmentIndexWriter::new(schema, index_writer_resource);
        let doc_count_publisher = DocCountPublisher::default();
        let building_segment = Arc::new(BuildingSegmentData::new(
            doc_count_publisher.reader(),
            column_writer.column_data(),
            index_writer.index_data(),
        ));

        Self {
            doc_count: 0,
            doc_count_publisher,
            column_writer,
            index_writer,
            building_segment_data: building_segment,
        }
    }

    pub fn add_document(&mut self, document: InnerInputDocument) {
        let docid = self.doc_count as DocId;
        // First column then index, so that indexed documents must be in column.
        self.column_writer.add_document(&document, docid);
        self.index_writer.add_document(&document, docid);
        self.doc_count += 1;
        self.doc_count_publisher.publish(self.doc_count);
    }

    pub fn building_segment_data(&self) -> &Arc<BuildingSegmentData> {
        &self.building_segment_data
    }
}
