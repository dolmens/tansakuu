use crate::{
    schema::Field,
    table::{SegmentMetaRegistry, TableData},
    types::PrimitiveType,
    DocId,
};

use super::{
    ColumnReader, PrimitiveColumnBuildingSegmentReader, PrimitiveColumnPersistentSegmentReader,
};

pub struct PrimitiveColumnReader<T: PrimitiveType> {
    segment_meta_registry: SegmentMetaRegistry,
    persistent_segments: Vec<PrimitiveColumnPersistentSegmentReader<T>>,
    building_segments: Vec<PrimitiveColumnBuildingSegmentReader<T::Native>>,
}

impl<T: PrimitiveType> PrimitiveColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let segment_meta_registry = table_data.segment_meta_registry().clone();

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
            segment_meta_registry,
            persistent_segments,
            building_segments,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<T::Native> {
        self.segment_meta_registry
            .locate_segment(docid)
            .and_then(|segment_cursor| {
                let base_docid = self
                    .segment_meta_registry
                    .segment(segment_cursor)
                    .base_docid();
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

impl<T: PrimitiveType> ColumnReader for PrimitiveColumnReader<T> {}
