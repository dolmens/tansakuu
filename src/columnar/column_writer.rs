use std::sync::Arc;

use crate::document::OwnedValue;

use super::ColumnBuildingSegmentData;

pub trait ColumnWriter {
    fn add_value(&mut self, value: &OwnedValue);
    fn column_data(&self) -> Arc<dyn ColumnBuildingSegmentData>;
}
