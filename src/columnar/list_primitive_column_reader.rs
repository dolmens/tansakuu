use std::sync::Arc;

use arrow::array::{ArrayRef, PrimitiveArray};

use crate::{schema::Field, table::TableData, types::PrimitiveType, DocId};

use super::{
    ColumnReader, ListColumnPersistentSegmentReader, ListPrimitiveColumnBuildingSegmentReader,
};

pub struct ListPrimitiveColumnReader<T: PrimitiveType> {
    persistent_segments: Vec<ListColumnPersistentSegmentReader>,
    building_segments: Vec<ListPrimitiveColumnBuildingSegmentReader<T::Native>>,
}

impl<T: PrimitiveType> ListPrimitiveColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let column_data = segment.data().column_data(field.name()).unwrap();
            let column_segment_reader = ListColumnPersistentSegmentReader::new(column_data);
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
            let list_primitive_column_data = column_data.downcast_arc().ok().unwrap();
            let column_segment_reader =
                ListPrimitiveColumnBuildingSegmentReader::new(list_primitive_column_data);
            building_segments.push(column_segment_reader);
        }

        Self {
            persistent_segments,
            building_segments,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<ArrayRef> {
        let mut docid = docid;
        for segment in &self.persistent_segments {
            if docid < segment.doc_count() as DocId {
                return segment.get(docid);
            }
            docid -= segment.doc_count() as DocId;
        }
        for segment in &self.building_segments {
            if docid < segment.doc_count() as DocId {
                return segment.get(docid).map(|data| {
                    Arc::new(PrimitiveArray::<T::ArrowPrimitive>::from_iter_values(
                        data.iter().copied(),
                    )) as ArrayRef
                });
            }
            docid -= segment.doc_count() as DocId;
        }

        None
    }
}

impl<T: PrimitiveType> ColumnReader for ListPrimitiveColumnReader<T> {}
