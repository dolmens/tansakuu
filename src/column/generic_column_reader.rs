use crate::{schema::Field, table::TableData, DocId};

use super::{ColumnReader, GenericColumnBuildingSegmentReader, GenericColumnSegmentReader};

pub struct GenericColumnReader<T> {
    segments: Vec<GenericColumnSegmentReader<T>>,
    building_segments: Vec<GenericColumnBuildingSegmentReader<T>>,
}

impl<T: Send + Sync + 'static> GenericColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut segments = vec![];
        for segment in table_data.segments() {
            let column_data = segment.segment().column_data(field.name());
            let generic_column_data = column_data.clone().downcast_arc().ok().unwrap();
            let column_segment_reader = GenericColumnSegmentReader::<T>::new(generic_column_data);
            segments.push(column_segment_reader);
        }

        let mut building_segments = vec![];
        for building_segment in table_data.building_segments() {
            let column_data = building_segment
                .segment()
                .column_data()
                .column_data(field.name())
                .unwrap();
            let generic_column_data = column_data.clone().downcast_arc().ok().unwrap();
            let column_segment_reader =
                GenericColumnBuildingSegmentReader::<T>::new(generic_column_data);
            building_segments.push(column_segment_reader);
        }

        Self {
            segments,
            building_segments,
        }
    }
}

impl<T: Send + Sync + 'static> ColumnReader for GenericColumnReader<T> {}

impl<T: Clone + Send + Sync + 'static> GenericColumnReader<T> {
    pub fn get(&self, docid: DocId) -> Option<T> {
        let mut docid = docid;
        for segment in &self.segments {
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
