use std::{path::Path, sync::Arc};

use tantivy_common::TerminatingWrite;

use crate::{schema::Field, Directory};

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
    fn serialize(&self, directory: &dyn Directory, column_directory: &Path) {
        let path = column_directory.join(&self.field_name);
        let writer = directory.open_write(&path).unwrap();
        let mut writer = GenericColumnSerializerWriter::<T>::new(writer);
        let values = &self.column_data.values;
        for value in values.iter() {
            writer.write(value.clone());
        }
        writer.finish().unwrap().terminate().unwrap();
    }
}
