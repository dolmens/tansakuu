use std::sync::Arc;

use crate::{document::OwnedValue, schema::FieldRef};

use super::ColumnBuildingSegmentData;

pub trait ColumnWriter {
    fn field(&self) -> &FieldRef;
    fn add_value(&mut self, value: Option<&OwnedValue>);
    fn column_data(&self) -> Arc<dyn ColumnBuildingSegmentData>;
}
