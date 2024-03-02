use std::sync::Arc;

use crate::{
    document::{OwnedValue, Value},
    util::chunked_vec::ChunkedVecWriter,
    BUILDING_COLUMN_VEC_CHUNK_SIZE, BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnBuildingSegmentData, ColumnWriter, StringColumnBuildingSegmentData};

pub struct StringColumnWriter {
    writer: ChunkedVecWriter<String>,
    column_data: Arc<StringColumnBuildingSegmentData>,
}

impl StringColumnWriter {
    pub fn new() -> Self {
        let writer = ChunkedVecWriter::new(
            BUILDING_COLUMN_VEC_CHUNK_SIZE,
            BUILDING_COLUMN_VEC_NODE_SIZE,
        );
        let reader = writer.reader();
        let column_data = Arc::new(StringColumnBuildingSegmentData::new(reader));

        Self {
            writer,
            column_data,
        }
    }
}

impl ColumnWriter for StringColumnWriter {
    fn add_value(&mut self, value: &OwnedValue) {
        self.writer.push(value.as_str().unwrap_or("").to_string());
    }

    fn column_data(&self) -> Arc<dyn ColumnBuildingSegmentData> {
        self.column_data.clone()
    }
}
