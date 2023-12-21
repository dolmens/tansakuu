use std::{str::FromStr, sync::Arc};

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

impl<T: FromStr + Send + Sync + 'static> ColumnWriter for GenericColumnWriter<T> {
    fn add_doc(&mut self, value: &str) {
        self.column_data.push(T::from_str(value).ok().unwrap());
    }

    fn column_data(&self) -> Arc<dyn ColumnSegmentData> {
        self.column_data.clone()
    }
}
