use crate::util::chunked_vec::ChunkedVec;

use super::ColumnBuildingSegmentData;

pub struct GeoLocationColumnBuildingSegmentData {
    pub values: ChunkedVec<Option<(f64, f64)>>,
    pub nullable: bool,
}

impl ColumnBuildingSegmentData for GeoLocationColumnBuildingSegmentData {}
