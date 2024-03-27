use std::sync::Arc;

use crate::{
    deletionmap::BuildingDeletionMapWriter, document::InnerInputDocument,
    index::IndexWriterResource, schema::SchemaRef, DocId, ESTIMATE_SEGMENT_DOC_COUNT,
    ESTIMATE_SEGMENT_INC_FACTOR,
};

use super::{BuildingSegmentData, DocCountPublisher, SegmentColumnWriter, SegmentIndexWriter};

pub struct SegmentWriter {
    doc_count: usize,
    doc_count_publisher: DocCountPublisher,
    column_writer: SegmentColumnWriter,
    index_writer: SegmentIndexWriter,
    deletionmap_writer: BuildingDeletionMapWriter,
    building_segment_data: Arc<BuildingSegmentData>,
}

impl SegmentWriter {
    pub fn new(schema: &SchemaRef, index_writer_resource: &IndexWriterResource) -> Self {
        let column_writer = SegmentColumnWriter::new(schema);
        let index_writer = SegmentIndexWriter::new(schema, index_writer_resource);
        let recent_segment_doc_count = index_writer_resource
            .recent_segment_stat()
            .map_or(0, |s| s.doc_count);
        let estimate_segment_doc_count = if recent_segment_doc_count > 0 {
            ((recent_segment_doc_count as f64) * ESTIMATE_SEGMENT_INC_FACTOR) as usize
        } else {
            ESTIMATE_SEGMENT_DOC_COUNT
        };
        let doc_count_publisher = DocCountPublisher::default();
        let deletionmap_writer =
            BuildingDeletionMapWriter::with_capacity(estimate_segment_doc_count);
        let building_segment = Arc::new(BuildingSegmentData::new(
            doc_count_publisher.reader(),
            column_writer.column_data(),
            index_writer.index_data(),
            deletionmap_writer.deletionmap(),
        ));

        Self {
            doc_count: 0,
            doc_count_publisher,
            column_writer,
            index_writer,
            deletionmap_writer,
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

    pub fn delete_document(&mut self, docid: DocId) {
        self.deletionmap_writer.delete_document(docid);
    }

    pub fn building_segment_data(&self) -> &Arc<BuildingSegmentData> {
        &self.building_segment_data
    }
}
