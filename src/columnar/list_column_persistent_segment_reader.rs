use arrow::array::{Array, ArrayRef, ListArray};

use crate::DocId;

use super::ColumnPersistentSegmentData;

pub struct ListColumnPersistentSegmentReader {
    values: ListArray,
}

impl ListColumnPersistentSegmentReader {
    pub fn new(column_data: &ColumnPersistentSegmentData) -> Self {
        let values = column_data
            .array()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone();

        Self { values }
    }

    pub fn get(&self, docid: DocId) -> Option<ArrayRef> {
        if self.values.is_null(docid as usize) {
            None
        } else {
            Some(self.values.value(docid as usize))
        }
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array, ListArray, ListBuilder, StringArray, StringBuilder},
        datatypes::Int32Type,
    };

    use crate::columnar::ColumnPersistentSegmentData;

    use super::ListColumnPersistentSegmentReader;

    #[test]
    fn test_basic_i32() {
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
        let column_data = ColumnPersistentSegmentData::new(Arc::new(list_array));
        let list_reader = ListColumnPersistentSegmentReader::new(&column_data);

        let expect0: Int32Array = vec![0, 1, 2].into();
        let expect0 = Arc::new(expect0);
        let expect2: Int32Array = vec![Some(3), None, Some(5)].into();
        let expect2 = Arc::new(expect2);
        let expect3: Int32Array = vec![6, 7].into();
        let expect3 = Arc::new(expect3);

        assert_eq!(list_reader.get(0), Some(expect0 as ArrayRef));
        assert_eq!(list_reader.get(1), None);
        assert_eq!(list_reader.get(2), Some(expect2 as ArrayRef));
        assert_eq!(list_reader.get(3), Some(expect3 as ArrayRef));
    }

    #[test]
    fn test_basic_string() {
        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);

        // [a, b, c]
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.values().append_value("c");
        builder.append(true);

        // Null
        builder.append(false);

        // [d]
        builder.values().append_value("d");
        builder.append(true);

        let array = builder.finish();

        let column_data = ColumnPersistentSegmentData::new(Arc::new(array));
        let list_reader = ListColumnPersistentSegmentReader::new(&column_data);

        let expect0 = StringArray::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let expect0 = Arc::new(expect0);
        let expect2 = StringArray::from(vec!["d".to_string()]);
        let expect2 = Arc::new(expect2);

        assert_eq!(list_reader.get(0), Some(expect0 as ArrayRef));
        assert_eq!(list_reader.get(1), None);
        assert_eq!(list_reader.get(2), Some(expect2 as ArrayRef));
    }
}
