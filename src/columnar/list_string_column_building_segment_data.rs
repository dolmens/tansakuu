use crate::util::chunked_vec::ChunkedVec;

use super::ColumnBuildingSegmentData;

pub struct ListStringColumnBuildingSegmentData {
    pub values: ChunkedVec<Option<Box<[String]>>>,
}

impl ListStringColumnBuildingSegmentData {
    pub fn new(values: ChunkedVec<Option<Box<[String]>>>) -> Self {
        Self { values }
    }
}

impl ColumnBuildingSegmentData for ListStringColumnBuildingSegmentData {}
