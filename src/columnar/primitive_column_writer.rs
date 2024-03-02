use std::sync::Arc;

use crate::{
    document::{OwnedValue, Value},
    types::NativeType,
    util::chunked_vec::ChunkedVecWriter,
    BUILDING_COLUMN_VEC_CHUNK_SIZE, BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnBuildingSegmentData, ColumnWriter, PrimitiveColumnBuildingSegmentData};

pub struct PrimitiveColumnWriter<T: NativeType> {
    writer: ChunkedVecWriter<T>,
    column_data: Arc<PrimitiveColumnBuildingSegmentData<T>>,
}

impl<T: NativeType> PrimitiveColumnWriter<T> {
    pub fn new() -> Self {
        let writer = ChunkedVecWriter::new(
            BUILDING_COLUMN_VEC_CHUNK_SIZE,
            BUILDING_COLUMN_VEC_NODE_SIZE,
        );
        let reader = writer.reader();
        let column_data = Arc::new(PrimitiveColumnBuildingSegmentData::new(reader));

        Self {
            writer,
            column_data,
        }
    }
}

macro_rules! impl_primitive_column_writer {
    ($ty:ty, $get_value:ident) => {
        impl ColumnWriter for PrimitiveColumnWriter<$ty> {
            fn add_value(&mut self, value: &OwnedValue) {
                self.writer.push(value.$get_value().unwrap_or_default());
            }

            fn column_data(&self) -> Arc<dyn ColumnBuildingSegmentData> {
                self.column_data.clone()
            }
        }
    };
}

impl_primitive_column_writer!(i64, as_i64);
