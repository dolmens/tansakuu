use crate::{
    schema::Field,
    table::{SegmentMetaRegistry, TableData},
    DocId,
};

use super::{
    BooleanColumnBuildingSegmentReader, BooleanColumnPersistentSegmentReader, ColumnReader,
};

pub struct BooleanColumnReader {
    segment_meta_registry: SegmentMetaRegistry,
    persistent_segments: Vec<BooleanColumnPersistentSegmentReader>,
    building_segments: Vec<BooleanColumnBuildingSegmentReader>,
}

impl BooleanColumnReader {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let segment_meta_registry = table_data.segment_meta_registry().clone();

        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let column_data = segment.data().column_data(field.name()).unwrap();
            let segment_reader = BooleanColumnPersistentSegmentReader::new(column_data);
            persistent_segments.push(segment_reader);
        }

        let mut building_segments = vec![];
        for building_segment in table_data.building_segments() {
            let column_data = building_segment
                .data()
                .column_data()
                .column_data(field.name())
                .cloned()
                .unwrap();
            let boolean_column_data = column_data.downcast_arc().ok().unwrap();
            let segment_reader = BooleanColumnBuildingSegmentReader::new(boolean_column_data);
            building_segments.push(segment_reader);
        }

        Self {
            segment_meta_registry,
            persistent_segments,
            building_segments,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<bool> {
        self.segment_meta_registry
            .locate_segment(docid)
            .and_then(|segment_cursor| {
                let base_docid = self
                    .segment_meta_registry
                    .segment_base_docid(segment_cursor);
                if segment_cursor < self.persistent_segments.len() {
                    let segment_reader = &self.persistent_segments[segment_cursor];
                    segment_reader.get(docid - base_docid)
                } else {
                    let segment_cursor = segment_cursor - self.persistent_segments.len();
                    let segment_reader = &self.building_segments[segment_cursor];
                    segment_reader.get(docid - base_docid)
                }
            })
    }
}

impl ColumnReader for BooleanColumnReader {}
