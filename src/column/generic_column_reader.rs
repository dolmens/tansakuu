use crate::{schema::Field, table::TableData, RowId};

use super::{
    ColumnReader, ColumnSegmentReader, GenericColumnBuildingSegmentReader,
    GenericColumnSegmentReader,
};

pub struct GenericColumnReader<T> {
    segments: Vec<GenericColumnSegmentReader<T>>,
    building_segments: Vec<GenericColumnBuildingSegmentReader<T>>,
}

impl<T: Send + Sync + 'static> GenericColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut segments = vec![];
        for segment in table_data.segments() {
            let column_data = segment.column_data(field.name());
            let generic_column_data = column_data.clone().downcast_arc().ok().unwrap();
            let column_segment_reader = GenericColumnSegmentReader::<T>::new(generic_column_data);
            segments.push(column_segment_reader);
        }

        let mut building_segments = vec![];
        for building_segment in table_data.building_segments() {
            let column_data = building_segment
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

    pub fn get(&self, rowid: RowId) -> Option<T>
    where
        T: Clone,
    {
        let mut base_docid = 0;
        for segment in &self.segments {
            if rowid - base_docid < segment.doc_count() {
                return segment.get(rowid - base_docid);
            }
            base_docid += segment.doc_count();
        }
        for segment in &self.building_segments {
            if rowid - base_docid < segment.doc_count() {
                return segment.get(rowid - base_docid);
            }
            base_docid += segment.doc_count();
        }

        None
    }
}

impl<T: Send + Sync + 'static> ColumnReader for GenericColumnReader<T> {}
