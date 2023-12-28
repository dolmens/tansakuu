use crate::{
    schema::Field,
    table::{TableData, TableDataSnapshot},
    RowId,
};

use super::{
    ColumnReader, GenericColumnBuildingSegmentReader, GenericColumnSegmentReader, TypedColumnReader,
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
}

impl<T: Send + Sync + 'static> ColumnReader for GenericColumnReader<T> {}

impl<T: Clone + Send + Sync + 'static> TypedColumnReader for GenericColumnReader<T> {
    type Item = T;
    fn get(&self, rowid: RowId, data_snapshot: &TableDataSnapshot) -> Option<Self::Item> {
        let mut rowid = rowid;
        let mut segment_cursor = 0;
        for segment in &self.segments {
            let segment_snapshot = &data_snapshot.segments[segment_cursor];
            if rowid < segment_snapshot.doc_count {
                return segment.get(rowid);
            }
            rowid -= segment_snapshot.doc_count;
            segment_cursor += 1;
        }
        for segment in &self.building_segments {
            let segment_snapshot = &data_snapshot.segments[segment_cursor];
            if rowid < segment_snapshot.doc_count {
                return segment.get(rowid);
            }
            rowid -= segment_snapshot.doc_count;
            segment_cursor += 1;
        }

        None
    }
}
