use crate::{util::chunked_vec::ChunkedVec, DocId};

use super::ColumnBuildingSegmentData;

pub struct StringColumnBuildingSegmentData {
    pub values: ChunkedVec<Option<String>>,
}

impl StringColumnBuildingSegmentData {
    pub fn new(values: ChunkedVec<Option<String>>) -> Self {
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<&str> {
        self.values
            .get(docid as usize)
            .unwrap()
            .as_ref()
            .map(|s| s.as_str())
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

impl ColumnBuildingSegmentData for StringColumnBuildingSegmentData {}

// #[cfg(test)]
// mod tests {
//     use std::error::Error;

//     use arrow::array::StringArray;

//     use crate::{
//         columnar::{ColumnBuildingSegmentData, StringColumnBuildingSegmentData},
//         util::chunked_vec::ChunkedVecWriter,
//     };

//     #[test]
//     fn test_basic() -> Result<(), Box<dyn Error>> {
//         let mut chunked_vec_writer = ChunkedVecWriter::<String>::new(4, 4);
//         chunked_vec_writer.push("hello".to_string());
//         chunked_vec_writer.push("world".to_string());
//         chunked_vec_writer.push("ok".to_string());
//         let values = chunked_vec_writer.reader();
//         let data = StringColumnBuildingSegmentData::new(values);
//         assert_eq!(data.doc_count(), 3);
//         let array = data.get_arrow_array();
//         let expect = StringArray::from(vec!["hello", "world", "ok"]);
//         assert_eq!(
//             array.as_any().downcast_ref::<StringArray>().unwrap(),
//             &expect
//         );

//         Ok(())
//     }
// }
