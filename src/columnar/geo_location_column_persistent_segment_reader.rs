use arrow::array::{Array, Float64Array, StructArray};

use crate::DocId;

use super::ColumnPersistentSegmentData;

pub struct GeoLocationColumnPersistentSegmentReader {
    values: StructArray,
    longitudes: Float64Array,
    latitudes: Float64Array,
}

impl GeoLocationColumnPersistentSegmentReader {
    pub fn new(column_data: &ColumnPersistentSegmentData) -> Self {
        let values = column_data
            .array()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();

        let longitudes = values
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        let latitudes = values
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();

        Self {
            values,
            longitudes,
            latitudes,
        }
    }

    pub fn get(&self, docid: DocId) -> Option<(f64, f64)> {
        if !self.values.is_null(docid as usize) {
            Some((
                self.longitudes.value(docid as usize),
                self.latitudes.value(docid as usize),
            ))
        } else {
            None
        }
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}
