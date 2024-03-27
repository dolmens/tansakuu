use crate::{
    schema::Field,
    table::{SegmentRegistry, TableData},
    types::PrimitiveType,
    DocId,
};

use super::{
    ColumnReader, PrimitiveColumnBuildingSegmentReader, PrimitiveColumnPersistentSegmentReader,
};

pub struct PrimitiveColumnReader<T: PrimitiveType> {
    segment_registry: SegmentRegistry,
    segment_readers: Vec<PrimitiveColumnSegmentReader<T>>,
}

enum PrimitiveColumnSegmentReader<T: PrimitiveType> {
    Persistent(PrimitiveColumnPersistentSegmentReader<T>),
    Building(PrimitiveColumnBuildingSegmentReader<T::Native>),
}

impl<T: PrimitiveType> PrimitiveColumnReader<T> {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut segment_registry = SegmentRegistry::default();
        let mut segment_readers = vec![];

        for segment in table_data.persistent_segments() {
            segment_registry.add_persistent_segment(segment.meta());
            let column_data = segment.data().column_data(field.name()).unwrap();
            let column_segment_reader =
                PrimitiveColumnPersistentSegmentReader::<T>::new(column_data);
            segment_readers.push(PrimitiveColumnSegmentReader::Persistent(
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
            let primitive_column_data = column_data.downcast_arc().ok().unwrap();
            let column_segment_reader =
                PrimitiveColumnBuildingSegmentReader::<T::Native>::new(primitive_column_data);
            segment_readers.push(PrimitiveColumnSegmentReader::Building(
                column_segment_reader,
            ));
        }

        Self {
            segment_registry,
            segment_readers,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<T::Native> {
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

impl<T: PrimitiveType> PrimitiveColumnSegmentReader<T> {
    fn get(&self, docid: DocId) -> Option<T::Native> {
        match self {
            Self::Persistent(persistent_segment_reader) => persistent_segment_reader.get(docid),
            Self::Building(building_segment_reader) => building_segment_reader.get(docid),
        }
    }
}

impl<T: PrimitiveType> ColumnReader for PrimitiveColumnReader<T> {}
