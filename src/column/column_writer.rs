use std::sync::Arc;

use crate::document::Value;

use super::ColumnSegmentData;

pub trait ColumnWriter {
    fn add_doc(&mut self, value: Value);
    fn column_data(&self) -> Arc<dyn ColumnSegmentData>;
}
