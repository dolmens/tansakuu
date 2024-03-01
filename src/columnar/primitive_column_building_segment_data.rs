use allocator_api2::alloc::Global;

use crate::{types::NativeType, util::chunked_vec::ChunkedVec, DocId};

use super::ColumnBuildingSegmentData;

pub struct PrimitiveColumnBuildingSegmentData<T: NativeType> {
    pub values: ChunkedVec<T, Global>,
}

impl<T: NativeType> PrimitiveColumnBuildingSegmentData<T> {
    pub fn new(values: ChunkedVec<T, Global>) -> Self {
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<T> {
        self.values.get(docid as usize).copied()
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

impl<T: NativeType> ColumnBuildingSegmentData for PrimitiveColumnBuildingSegmentData<T> {}

// #[cfg(test)]
// mod tests {
//     use std::error::Error;

//     use arrow::array::Int64Array;

//     use crate::{
//         columnar::{ColumnBuildingSegmentData, PrimitiveColumnBuildingSegmentDataOLD},
//         types::Int64Type,
//         util::chunked_vec::ChunkedVecWriter,
//     };

//     #[test]
//     fn test_basic() -> Result<(), Box<dyn Error>> {
//         let mut chunked_vec_writer = ChunkedVecWriter::<i64>::new(4, 4);
//         chunked_vec_writer.push(100);
//         chunked_vec_writer.push(300);
//         chunked_vec_writer.push(200);
//         let values = chunked_vec_writer.reader();
//         let data = PrimitiveColumnBuildingSegmentDataOLD::<Int64Type>::new(values);
//         assert_eq!(data.doc_count(), 3);
//         let array = data.get_arrow_array();
//         assert_eq!(
//             array
//                 .as_any()
//                 .downcast_ref::<Int64Array>()
//                 .unwrap()
//                 .values(),
//             &[100, 300, 200]
//         );

//         Ok(())
//     }
// }
