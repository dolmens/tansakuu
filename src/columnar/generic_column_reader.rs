use crate::{schema::Field, table::TableData, DocId};

use super::{
    ColumnReader, GenericColumnBuildingSegmentReader, GenericColumnPersistentSegmentReader,
};

pub struct GenericColumnReader<T> {
    persistent_segments: Vec<GenericColumnPersistentSegmentReader<T>>,
    building_segments: Vec<GenericColumnBuildingSegmentReader<T>>,
}

impl<T: Clone + Send + Sync + 'static> GenericColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        unimplemented!()
        // let mut persistent_segments = vec![];
        // for segment in table_data.persistent_segments() {
        //     let column_data = segment.data().column_data(field.name());
        //     let generic_column_data = column_data.clone().downcast_arc().ok().unwrap();
        //     let column_segment_reader =
        //         GenericColumnPersistentSegmentReader::<T>::new(generic_column_data);
        //     persistent_segments.push(column_segment_reader);
        // }

        // let mut building_segments = vec![];
        // for building_segment in table_data.building_segments() {
        //     let column_data = building_segment
        //         .data()
        //         .column_data()
        //         .column_data(field.name())
        //         .cloned()
        //         .unwrap();
        //     let generic_column_data = column_data.downcast_arc().ok().unwrap();
        //     let column_segment_reader =
        //         GenericColumnBuildingSegmentReader::<T>::new(generic_column_data);
        //     building_segments.push(column_segment_reader);
        // }

        // Self {
        //     persistent_segments,
        //     building_segments,
        // }
    }
}

impl<T: Send + Sync + 'static> ColumnReader for GenericColumnReader<T> {}

impl<T: Clone + Send + Sync + 'static> GenericColumnReader<T> {
    pub fn get(&self, docid: DocId) -> Option<T> {
        let mut docid = docid;
        for segment in &self.persistent_segments {
            if docid < segment.doc_count() as DocId {
                return segment.get(docid);
            }
            docid -= segment.doc_count() as DocId;
        }
        for segment in &self.building_segments {
            if docid < segment.doc_count() as DocId {
                return segment.get(docid).cloned();
            }
            docid -= segment.doc_count() as DocId;
        }

        None
    }
}
