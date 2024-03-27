use crate::{
    schema::Field,
    table::{SegmentRegistry, TableData},
    DocId,
};

use super::{
    ColumnReader, GeoLocationColumnBuildingSegmentReader, GeoLocationColumnPersistentSegmentReader,
};

pub struct GeoLocationColumnReader {
    segment_registry: SegmentRegistry,
    segment_readers: Vec<GeoLocationColumnSegmentReader>,
}

enum GeoLocationColumnSegmentReader {
    Persistent(GeoLocationColumnPersistentSegmentReader),
    Building(GeoLocationColumnBuildingSegmentReader),
}

impl GeoLocationColumnReader {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut segment_registry = SegmentRegistry::default();
        let mut segment_readers = vec![];
        for segment in table_data.persistent_segments() {
            segment_registry.add_persistent_segment(segment.meta());
            let column_data = segment.data().column_data(field.name()).unwrap();
            let column_segment_reader = GeoLocationColumnPersistentSegmentReader::new(column_data);
            segment_readers.push(GeoLocationColumnSegmentReader::Persistent(
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
            let geo_location_column_data = column_data.downcast_arc().ok().unwrap();
            let column_segment_reader =
                GeoLocationColumnBuildingSegmentReader::new(geo_location_column_data);
            segment_readers.push(GeoLocationColumnSegmentReader::Building(
                column_segment_reader,
            ));
        }

        Self {
            segment_registry,
            segment_readers,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<(f64, f64)> {
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

impl GeoLocationColumnSegmentReader {
    fn get(&self, docid: DocId) -> Option<(f64, f64)> {
        match self {
            Self::Persistent(segment_reader) => segment_reader.get(docid),
            Self::Building(segment_reader) => segment_reader.get(docid),
        }
    }
}

impl ColumnReader for GeoLocationColumnReader {}
