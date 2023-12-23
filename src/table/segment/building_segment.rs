use std::sync::Arc;

use super::{BuildingSegmentColumnData, BuildingSegmentData, BuildingSegmentIndexData};

pub struct BuildingSegment {
    segment_data: Arc<BuildingSegmentData>,
}

impl BuildingSegment {
    pub fn new(segment_data: Arc<BuildingSegmentData>) -> Self {
        Self { segment_data }
    }

    pub fn doc_count(&self) -> usize {
        self.segment_data.doc_count()
    }

    pub fn column_data(&self) -> &BuildingSegmentColumnData {
        self.segment_data.column_data()
    }

    pub fn index_data(&self) -> &BuildingSegmentIndexData {
        self.segment_data.index_data()
    }
}
