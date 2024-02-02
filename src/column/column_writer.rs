use std::sync::Arc;

use crate::document::OwnedValue;

use super::ColumnSegmentData;

pub trait ColumnWriter {
    fn add_doc(&mut self, value: OwnedValue);
    fn column_data(&self) -> Arc<dyn ColumnSegmentData>;
}
