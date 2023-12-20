use std::sync::Arc;

use super::ColumnSegmentData;

pub trait ColumnWriter {
    fn add_doc(&mut self, value: &str);
    fn column_data(&self) -> Arc<dyn ColumnSegmentData>;
}
