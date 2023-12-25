use super::{BuildingSegmentColumnData, BuildingSegmentData, BuildingSegmentIndexData};

pub struct BuildingSegment {
    segment_data: BuildingSegmentData,
}

impl BuildingSegment {
    pub fn new(segment_data: BuildingSegmentData) -> Self {
        Self { segment_data }
    }

    pub fn doc_count(&self) -> usize {
        self.segment_data.doc_count()
    }

    pub fn segment_data(&self) -> &BuildingSegmentData {
        &self.segment_data
    }

    pub fn column_data(&self) -> &BuildingSegmentColumnData {
        self.segment_data.column_data()
    }

    pub fn index_data(&self) -> &BuildingSegmentIndexData {
        self.segment_data.index_data()
    }
}
