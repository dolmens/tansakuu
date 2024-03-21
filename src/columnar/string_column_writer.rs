use std::sync::Arc;

use crate::{
    document::{OwnedValue, Value},
    schema::FieldRef,
    util::chunked_vec::ChunkedVecWriter,
    BUILDING_COLUMN_VEC_CHUNK_SIZE, BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnBuildingSegmentData, ColumnWriter, StringColumnBuildingSegmentData};

pub struct StringColumnWriter {
    writer: ChunkedVecWriter<Option<String>>,
    column_data: Arc<StringColumnBuildingSegmentData>,
    field: FieldRef,
}

impl StringColumnWriter {
    pub fn new(field: FieldRef) -> Self {
        let writer = ChunkedVecWriter::new(
            BUILDING_COLUMN_VEC_CHUNK_SIZE,
            BUILDING_COLUMN_VEC_NODE_SIZE,
        );
        let reader = writer.reader();
        let column_data = Arc::new(StringColumnBuildingSegmentData::new(reader));

        Self {
            field,
            writer,
            column_data,
        }
    }
}

impl ColumnWriter for StringColumnWriter {
    fn field(&self) -> &FieldRef {
        &self.field
    }

    fn add_value(&mut self, value: Option<&OwnedValue>) {
        if let Some(value) = value.map(|value| value.as_str()).flatten() {
            self.writer.push(Some(value.to_string()));
        } else {
            if self.field.is_nullable() {
                self.writer.push(None);
            } else {
                self.writer.push(Some(Default::default()));
            }
        }
    }

    fn column_data(&self) -> Arc<dyn ColumnBuildingSegmentData> {
        self.column_data.clone()
    }
}
