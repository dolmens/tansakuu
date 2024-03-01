use crate::{schema::Field, table::TableData, DocId};

use super::{ColumnReader, StringColumnBuildingSegmentReader, StringColumnPersistentSegmentReader};

pub struct StringColumnReader {
    persistent_segments: Vec<StringColumnPersistentSegmentReader>,
    building_segments: Vec<StringColumnBuildingSegmentReader>,
}

impl StringColumnReader {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let column_data = segment.data().column_data(field.name()).unwrap();
            let column_segment_reader = StringColumnPersistentSegmentReader::new(column_data);
            persistent_segments.push(column_segment_reader);
        }

        let mut building_segments = vec![];
        for building_segment in table_data.building_segments() {
            let column_data = building_segment
                .data()
                .column_data()
                .column_data(field.name())
                .cloned()
                .unwrap();
            let string_column_data = column_data.downcast_arc().ok().unwrap();
            let column_segment_reader = StringColumnBuildingSegmentReader::new(string_column_data);
            building_segments.push(column_segment_reader);
        }

        Self {
            persistent_segments,
            building_segments,
        }
    }
}

impl ColumnReader for StringColumnReader {}

impl StringColumnReader {
    pub fn get(&self, docid: DocId) -> Option<&str> {
        let mut docid = docid;
        for segment in &self.persistent_segments {
            if docid < segment.doc_count() as DocId {
                return segment.get(docid);
            }
            docid -= segment.doc_count() as DocId;
        }
        for segment in &self.building_segments {
            if docid < segment.doc_count() as DocId {
                return segment.get(docid);
            }
            docid -= segment.doc_count() as DocId;
        }

        None
    }
}
