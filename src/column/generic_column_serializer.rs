use std::{path::Path, sync::Arc};

use crate::schema::Field;

use super::{
    column_serializer::ColumnSerializer, GenericColumnBuildingSegmentData,
    GenericColumnSerializerWriter,
};

pub struct GenericColumnSerializer<T> {
    field_name: String,
    column_data: Arc<GenericColumnBuildingSegmentData<T>>,
}

impl<T> GenericColumnSerializer<T> {
    pub fn new(field: &Field, column_data: Arc<GenericColumnBuildingSegmentData<T>>) -> Self {
        Self {
            field_name: field.name().to_string(),
            column_data,
        }
    }
}

impl<T: Clone + ToString> ColumnSerializer for GenericColumnSerializer<T> {
    fn serialize(&self, directory: &Path) {
        let path = directory.join(&self.field_name);
        let mut writer = GenericColumnSerializerWriter::<T>::new(path);
        let values = self.column_data.values();
        for value in &values {
            writer.write(value.clone());
        }
    }
}
