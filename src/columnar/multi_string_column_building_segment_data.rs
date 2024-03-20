use crate::util::chunked_vec::ChunkedVec;

use super::ColumnBuildingSegmentData;

pub struct MultiStringColumnBuildingSegmentData {
    pub values: ChunkedVec<Option<Box<[String]>>>,
}

impl MultiStringColumnBuildingSegmentData {
    pub fn new(values: ChunkedVec<Option<Box<[String]>>>) -> Self {
        Self { values }
    }
}

impl ColumnBuildingSegmentData for MultiStringColumnBuildingSegmentData {}
