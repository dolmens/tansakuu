use std::sync::Arc;

use crate::{
    document::Value, schema::FieldRef, util::chunked_vec::ChunkedVecWriter,
    BUILDING_COLUMN_VEC_CHUNK_SIZE, BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnWriter, ListStringColumnBuildingSegmentData};

pub struct ListStringColumnWriter {
    field: FieldRef,
    writer: ChunkedVecWriter<Option<Box<[String]>>>,
    column_data: Arc<ListStringColumnBuildingSegmentData>,
}

impl ListStringColumnWriter {
    pub fn new(field: FieldRef) -> Self {
        let writer = ChunkedVecWriter::new(
            BUILDING_COLUMN_VEC_CHUNK_SIZE,
            BUILDING_COLUMN_VEC_NODE_SIZE,
        );
        let reader = writer.reader();
        let column_data = Arc::new(ListStringColumnBuildingSegmentData::new(reader));

        Self {
            field,
            writer,
            column_data,
        }
    }
}

impl ColumnWriter for ListStringColumnWriter {
    fn field(&self) -> &FieldRef {
        &self.field
    }

    fn add_value(&mut self, value: Option<&crate::document::OwnedValue>) {
        if let Some(iter) = value.map(|value| value.as_array()).flatten() {
            let values: Vec<_> = iter
                .map(|elem| elem.as_str().unwrap_or("").to_string())
                .collect();
            self.writer.push(Some(values.into_boxed_slice()));
        } else {
            if self.field.is_nullable() {
                self.writer.push(None);
            } else {
                self.writer.push(Some(vec![].into_boxed_slice()));
            }
        }
    }

    fn column_data(&self) -> Arc<dyn super::ColumnBuildingSegmentData> {
        self.column_data.clone()
    }
}
