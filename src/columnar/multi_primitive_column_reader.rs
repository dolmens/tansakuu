use std::sync::Arc;

use arrow::array::{ArrayRef, PrimitiveArray};

use crate::{
    schema::Field,
    table::{SegmentRegistry, TableData},
    types::PrimitiveType,
    DocId,
};

use super::{
    ColumnReader, MultiColumnPersistentSegmentReader, MultiPrimitiveColumnBuildingSegmentReader,
};

pub struct MultiPrimitiveColumnReader<T: PrimitiveType> {
    segment_registry: SegmentRegistry,
    segment_readers: Vec<MultiPrimitiveColumnSegmentReader<T>>,
}

enum MultiPrimitiveColumnSegmentReader<T: PrimitiveType> {
    Persistent(MultiColumnPersistentSegmentReader),
    Building(MultiPrimitiveColumnBuildingSegmentReader<T::Native>),
}

impl<T: PrimitiveType> MultiPrimitiveColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut segment_registry = SegmentRegistry::default();
        let mut segment_readers = vec![];

        for segment in table_data.persistent_segments() {
            segment_registry.add_persistent_segment(segment.meta());
            let column_data = segment.data().column_data(field.name()).unwrap();
            let column_segment_reader = MultiColumnPersistentSegmentReader::new(column_data);
            segment_readers.push(MultiPrimitiveColumnSegmentReader::Persistent(
                column_segment_reader,
            ));
        }

        for building_segment in table_data.building_segments() {
            segment_registry
                .add_building_segment(building_segment.meta(), building_segment.data().doc_count());
            let column_data = building_segment
                .data()
                .column_data()
                .column_data(field.name())
                .cloned()
                .unwrap();
            let list_primitive_column_data = column_data.downcast_arc().ok().unwrap();
            let column_segment_reader =
                MultiPrimitiveColumnBuildingSegmentReader::new(list_primitive_column_data);
            segment_readers.push(MultiPrimitiveColumnSegmentReader::Building(
                column_segment_reader,
            ));
        }

        Self {
            segment_registry,
            segment_readers,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<ArrayRef> {
        self.segment_registry
            .locate_segment(docid)
            .and_then(|segment_cursor| {
                let docid_in_segment = self
                    .segment_registry
                    .docid_in_segment(docid, segment_cursor);
                self.segment_readers[segment_cursor].get(docid_in_segment)
            })
    }
}

impl<T: PrimitiveType> MultiPrimitiveColumnSegmentReader<T> {
    fn get(&self, docid: DocId) -> Option<ArrayRef> {
        match self {
            Self::Persistent(segment_reader) => segment_reader.get(docid),
            Self::Building(segment_reader) => segment_reader.get(docid).map(|data| {
                Arc::new(PrimitiveArray::<T::ArrowPrimitive>::from_iter_values(
                    data.iter().copied(),
                )) as ArrayRef
            }),
        }
    }
}

impl<T: PrimitiveType> ColumnReader for MultiPrimitiveColumnReader<T> {}
