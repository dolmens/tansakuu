use crate::{schema::Field, table::TableData, RowId};

use super::{
    ColumnReader, ColumnSegmentReader, GenericColumnSegmentData, GenericColumnSegmentReader,
};

pub struct GenericColumnReader<T> {
    segments: Vec<GenericColumnSegmentReader<T>>,
}

impl<T: Send + Sync + 'static> GenericColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut segments = vec![];
        for building_segment in table_data
            .dumping_segments()
            .chain(table_data.building_segments())
        {
            let column_data = building_segment
                .column_data()
                .column_data(field.name())
                .unwrap();
            let generic_column_data = column_data
                .clone()
                .downcast_arc::<GenericColumnSegmentData<T>>()
                .ok()
                .unwrap();
            let column_segment_reader = GenericColumnSegmentReader::<T>::new(generic_column_data);
            segments.push(column_segment_reader);
        }

        Self { segments }
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

        None
    }
}

impl<T: Send + Sync + 'static> ColumnReader for GenericColumnReader<T> {}
