use super::{BuildingSegmentColumnData, BuildingSegmentIndexData};

pub struct BuildingSegmentData {
    column_data: BuildingSegmentColumnData,
    index_data: BuildingSegmentIndexData,
}

impl BuildingSegmentData {
    pub fn new(
        column_data: BuildingSegmentColumnData,
        index_data: BuildingSegmentIndexData,
    ) -> Self {
        Self {
            column_data,
            index_data,
        }
    }

    pub fn column_data(&self) -> &BuildingSegmentColumnData {
        &self.column_data
    }

    pub fn index_data(&self) -> &BuildingSegmentIndexData {
        &self.index_data
    }
}
