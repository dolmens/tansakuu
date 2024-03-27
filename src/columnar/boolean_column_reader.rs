use crate::{
    schema::Field,
    table::{SegmentRegistry, TableData},
    DocId,
};

use super::{
    BooleanColumnBuildingSegmentReader, BooleanColumnPersistentSegmentReader, ColumnReader,
};

pub struct BooleanColumnReader {
    segment_registry: SegmentRegistry,
    segment_readers: Vec<BooleanColumnSegmentReader>,
}

enum BooleanColumnSegmentReader {
    Persistent(BooleanColumnPersistentSegmentReader),
    Building(BooleanColumnBuildingSegmentReader),
}

impl BooleanColumnReader {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut segment_registry = SegmentRegistry::default();
        let mut segment_readers = vec![];

        for segment in table_data.persistent_segments() {
            segment_registry.add_persistent_segment(segment.meta());
            let column_data = segment.data().column_data(field.name()).unwrap();
            let segment_reader = BooleanColumnPersistentSegmentReader::new(column_data);
            segment_readers.push(BooleanColumnSegmentReader::Persistent(segment_reader));
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
            let boolean_column_data = column_data.downcast_arc().ok().unwrap();
            let segment_reader = BooleanColumnBuildingSegmentReader::new(boolean_column_data);
            segment_readers.push(BooleanColumnSegmentReader::Building(segment_reader));
        }

        Self {
            segment_registry,
            segment_readers,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<bool> {
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

impl BooleanColumnSegmentReader {
    fn get(&self, docid: DocId) -> Option<bool> {
        match self {
            Self::Persistent(segment_reader) => segment_reader.get(docid),
            Self::Building(segment_reader) => segment_reader.get(docid),
        }
    }
}

impl ColumnReader for BooleanColumnReader {}
