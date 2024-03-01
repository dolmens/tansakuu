use arrow::array::ArrayRef;

use super::ColumnBuildingSegmentData;

pub trait ColumnSerializer {
    fn serialize(&self, column_data: &dyn ColumnBuildingSegmentData) -> ArrayRef;
}
