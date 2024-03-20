use std::sync::Arc;

use crate::{util::chunked_vec::ChunkedVec, DocId};

use super::GeoLocationColumnBuildingSegmentData;

pub struct GeoLocationColumnBuildingSegmentReader {
    values: ChunkedVec<Option<(f64, f64)>>,
}

impl GeoLocationColumnBuildingSegmentReader {
    pub fn new(column_data: Arc<GeoLocationColumnBuildingSegmentData>) -> Self {
        let values = column_data.values.clone();
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<(f64, f64)> {
        self.values
            .get(docid as usize)
            .unwrap()
            .as_ref()
            .map(|&(lon, lat)| (lon, lat))
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}
