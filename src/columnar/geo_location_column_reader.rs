use crate::{schema::Field, table::TableData, DocId};

use super::{
    ColumnReader, GeoLocationColumnBuildingSegmentReader, GeoLocationColumnPersistentSegmentReader,
};

pub struct GeoLocationColumnReader {
    persistent_segments: Vec<GeoLocationColumnPersistentSegmentReader>,
    building_segments: Vec<GeoLocationColumnBuildingSegmentReader>,
}

impl GeoLocationColumnReader {
    pub fn new(field: &Field, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let column_data = segment.data().column_data(field.name()).unwrap();
            let column_segment_reader = GeoLocationColumnPersistentSegmentReader::new(column_data);
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
            let geo_location_column_data = column_data.downcast_arc().ok().unwrap();
            let column_segment_reader =
                GeoLocationColumnBuildingSegmentReader::new(geo_location_column_data);
            building_segments.push(column_segment_reader);
        }

        Self {
            persistent_segments,
            building_segments,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<(f64, f64)> {
        let mut docid = docid;
        for segment in &self.persistent_segments {
            if docid < segment.doc_count() as DocId {
                return segment.get(docid);
            }
            docid -= segment.doc_count() as DocId;
        }
        for segment in &self.building_segments {
            if docid < segment.doc_count() as DocId {
                return segment.get(docid);
            }
            docid -= segment.doc_count() as DocId;
        }

        None
    }
}

impl ColumnReader for GeoLocationColumnReader {}
