use crate::{schema::Field, table::TableData, types::PrimitiveType, DocId};

use super::{
    ColumnReader, PrimitiveColumnBuildingSegmentReader, PrimitiveColumnPersistentSegmentReader,
};

pub struct PrimitiveColumnReader<T: PrimitiveType> {
    persistent_segments: Vec<PrimitiveColumnPersistentSegmentReader<T>>,
    building_segments: Vec<PrimitiveColumnBuildingSegmentReader<T::Native>>,
}

impl<T: PrimitiveType> PrimitiveColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let column_data = segment.data().column_data(field.name()).unwrap();
            let column_segment_reader =
                PrimitiveColumnPersistentSegmentReader::<T>::new(column_data);
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
            let primitive_column_data = column_data.downcast_arc().ok().unwrap();
            let column_segment_reader =
                PrimitiveColumnBuildingSegmentReader::<T::Native>::new(primitive_column_data);
            building_segments.push(column_segment_reader);
        }

        Self {
            persistent_segments,
            building_segments,
        }
    }
}

impl<T: PrimitiveType> ColumnReader for PrimitiveColumnReader<T> {}

impl<T: PrimitiveType> PrimitiveColumnReader<T> {
    pub fn get(&self, docid: DocId) -> Option<T::Native> {
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
