use std::sync::Arc;

use allocator_api2::alloc::Global;

use crate::{
    document::OwnedValue, util::chunked_vec::ChunkedVecWriter, BUILDING_COLUMN_VEC_CHUNK_SIZE,
    BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnSegmentData, ColumnWriter, GenericColumnBuildingSegmentData};

pub struct GenericColumnWriter<T> {
    writer: ChunkedVecWriter<T, Global>,
    column_data: Arc<GenericColumnBuildingSegmentData<T>>,
}

impl<T> GenericColumnWriter<T> {
    pub fn new() -> Self {
        let writer = ChunkedVecWriter::new(
            BUILDING_COLUMN_VEC_CHUNK_SIZE,
            BUILDING_COLUMN_VEC_NODE_SIZE,
        );
        let reader = writer.reader();
        let column_data = Arc::new(GenericColumnBuildingSegmentData::new(reader));

        Self {
            writer,
            column_data,
        }
    }
}

impl<T: Send + Sync + 'static> ColumnWriter for GenericColumnWriter<T>
where
    OwnedValue: TryInto<T>,
{
    fn add_doc(&mut self, value: OwnedValue) {
        self.writer.push(value.try_into().ok().unwrap());
    }

    fn column_data(&self) -> Arc<dyn ColumnSegmentData> {
        self.column_data.clone()
    }
}
