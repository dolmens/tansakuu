use std::sync::Arc;

use arrow::array::StringArray;

use crate::{types::NativeType, util::chunked_vec::ChunkedVec, DocId};

use super::ListPrimitiveColumnBuildingSegmentData;

pub struct ListPrimitiveColumnBuildingSegmentReader<T: NativeType> {
    values: ChunkedVec<Option<Box<[T]>>>,
}

impl<T: NativeType> ListPrimitiveColumnBuildingSegmentReader<T>
where
    StringArray: for<'a> From<Vec<&'a str>>,
{
    pub fn new(column_data: Arc<ListPrimitiveColumnBuildingSegmentData<T>>) -> Self {
        let values = column_data.values.clone();
        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<&[T]> {
        self.values.get(docid as usize).unwrap().as_deref()
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use crate::{
        columnar::ListPrimitiveColumnBuildingSegmentData, util::chunked_vec::ChunkedVecWriter,
    };

    use super::ListPrimitiveColumnBuildingSegmentReader;

    #[test]
    fn test_basic() -> Result<(), Box<dyn Error>> {
        let mut chunked_vec_writer = ChunkedVecWriter::<Option<Box<[i32]>>>::new(4, 4);
        chunked_vec_writer.push(Some(vec![1, 3, 5].into_boxed_slice()));
        chunked_vec_writer.push(None);
        chunked_vec_writer.push(Some(vec![4].into_boxed_slice()));
        let values = chunked_vec_writer.reader();
        let column_data = Arc::new(ListPrimitiveColumnBuildingSegmentData::new(values));
        let reader = ListPrimitiveColumnBuildingSegmentReader::new(column_data);

        assert_eq!(reader.doc_count(), 3);

        let expect0 = Some(vec![1, 3, 5]);
        let expect0 = expect0.as_ref().map(|v| v.as_slice());
        assert_eq!(reader.get(0), expect0);

        assert_eq!(reader.get(1), None);

        let expect2 = Some(vec![4]);
        let expect2 = expect2.as_ref().map(|v| v.as_slice());
        assert_eq!(reader.get(2), expect2);
        Ok(())
    }
}
