use std::{marker::PhantomData, sync::Arc};

use allocator_api2::alloc::Global;

use crate::{
    document::{OwnedValue, Value},
    types::NativeType,
    util::chunked_vec::ChunkedVecWriter,
    BUILDING_COLUMN_VEC_CHUNK_SIZE, BUILDING_COLUMN_VEC_NODE_SIZE,
};

use super::{ColumnBuildingSegmentData, ColumnWriter, PrimitiveColumnBuildingSegmentData};

pub struct PrimitiveColumnWriter<T: NativeType> {
    writer: ChunkedVecWriter<T, Global>,
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

// struct PrimitiveValueAccessor<T: NativeType> {
//     _marker: PhantomData<T>,
// }

// impl<T: NativeType> Default for PrimitiveValueAccessor<T> {
//     fn default() -> Self {
//         Self {
//             _marker: PhantomData,
//         }
//     }
// }

// impl PrimitiveValueAccessor<i64> {
//     fn get(&self, value: &OwnedValue) -> i64 {
//         value.as_i64().unwrap_or_default()
//     }
// }

// impl<T: NativeType> ColumnWriter for PrimitiveColumnWriter<T> {
//     fn add_value(&mut self, value: OwnedValue) {
//         let pp = PrimitiveValueAccessor::<T>::default().get(&value);
//         unimplemented!()
//         // self.writer.push(value.try_into().ok().unwrap());
//     }

//     fn column_data(&self) -> Arc<dyn ColumnBuildingSegmentData> {
//         self.column_data.clone()
//     }
// }

impl ColumnWriter for PrimitiveColumnWriter<i64> {
    fn add_value(&mut self, value: &OwnedValue) {
        self.writer.push(value.as_i64().unwrap_or_default());
    }

    fn column_data(&self) -> Arc<dyn ColumnBuildingSegmentData> {
        self.column_data.clone()
    }
}
