use std::sync::Arc;

use allocator_api2::alloc::Global;
use arrow::array::StringArray;

use crate::{util::chunked_vec::ChunkedVec, DocId};

use super::StringColumnBuildingSegmentData;

pub struct StringColumnBuildingSegmentReader {
    values: ChunkedVec<String, Global>,
}

impl StringColumnBuildingSegmentReader
where
    StringArray: for<'a> From<Vec<&'a str>>,
{
    pub fn new(column_data: Arc<StringColumnBuildingSegmentData>) -> Self {
        let values = column_data.values.clone();
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<&str> {
        self.values.get(docid as usize).map(|s| s.as_str())
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use crate::{columnar::StringColumnBuildingSegmentData, util::chunked_vec::ChunkedVecWriter};

    use super::StringColumnBuildingSegmentReader;

    #[test]
    fn test_basic() -> Result<(), Box<dyn Error>> {
        let mut chunked_vec_writer = ChunkedVecWriter::<String>::new(4, 4);
        chunked_vec_writer.push("hello".to_string());
        chunked_vec_writer.push("world".to_string());
        chunked_vec_writer.push("ok".to_string());
        let values = chunked_vec_writer.reader();
        let column_data = Arc::new(StringColumnBuildingSegmentData::new(values));
        let reader = StringColumnBuildingSegmentReader::new(column_data);
        assert_eq!(reader.doc_count(), 3);
        assert_eq!(reader.get(0), Some("hello"));
        assert_eq!(reader.get(2), Some("ok"));

        Ok(())
    }
}
