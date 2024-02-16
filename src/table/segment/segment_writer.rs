use std::sync::Arc;

use crate::{deletionmap::DeletionMapWriter, document::Document, schema::SchemaRef, DocId};

use super::{BuildingSegmentData, SegmentColumnWriter, SegmentId, SegmentIndexWriter, SegmentStat};

pub struct SegmentWriter {
    doc_count: usize,
    column_writer: SegmentColumnWriter,
    index_writer: SegmentIndexWriter,
    deletionmap_writer: DeletionMapWriter,
    building_segment: Arc<BuildingSegmentData>,
}

impl SegmentWriter {
    pub fn new(schema: &SchemaRef, recent_segment_stat: Option<&Arc<SegmentStat>>) -> Self {
        let column_writer = SegmentColumnWriter::new(schema);
        let index_writer = SegmentIndexWriter::new(schema, recent_segment_stat);
        let deletionmap_writer = DeletionMapWriter::new();
        let building_segment = Arc::new(BuildingSegmentData::new(
            column_writer.column_data(),
            index_writer.index_data(),
            deletionmap_writer.deletionmap(),
        ));

        Self {
            doc_count: 0,
            column_writer,
            index_writer,
            deletionmap_writer,
            building_segment,
        }
    }

    pub fn add_doc<D: Document>(&mut self, doc: &D) {
        let docid = self.doc_count as DocId;
        // First column then index, so that indexed documents must be in column.
        self.column_writer.add_doc(doc, docid);
        self.index_writer.add_doc(doc, docid);
        self.doc_count += 1;
        self.building_segment.set_doc_count(self.doc_count);
    }

    pub fn delete_doc(&mut self, segment_id: SegmentId, docid: DocId) {
        self.deletionmap_writer.delete_doc(segment_id, docid);
    }

    pub fn building_segment(&self) -> &Arc<BuildingSegmentData> {
        &self.building_segment
    }
}
