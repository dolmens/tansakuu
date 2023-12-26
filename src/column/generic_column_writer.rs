use std::sync::Arc;

use crate::document::Value;

use super::{ColumnSegmentData, ColumnWriter, GenericColumnBuildingSegmentData};

pub struct GenericColumnWriter<T> {
    column_data: Arc<GenericColumnBuildingSegmentData<T>>,
}

impl<T> GenericColumnWriter<T> {
    pub fn new() -> Self {
        Self {
            column_data: Arc::new(GenericColumnBuildingSegmentData::new()),
        }
    }
}

impl<T: Send + Sync + 'static> ColumnWriter for GenericColumnWriter<T>
where
    Value: TryInto<T>,
{
    fn add_doc(&mut self, value: Value) {
        self.column_data.push(value.try_into().ok().unwrap());
    }

    fn column_data(&self) -> Arc<dyn ColumnSegmentData> {
        self.column_data.clone()
    }
}
