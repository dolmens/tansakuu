use std::sync::Arc;

use crate::columnar::ColumnReader;

pub struct PrimaryKeyReader(Arc<dyn ColumnReader>);

impl PrimaryKeyReader {
    pub fn new(primary_key_reader: Arc<dyn ColumnReader>) -> Self {
        Self(primary_key_reader)
    }

    pub fn typed_reader<T: ColumnReader>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}
