use arrow::array::ArrayRef;

pub struct ColumnPersistentSegmentData {
    values: ArrayRef,
}

impl ColumnPersistentSegmentData {
    pub fn new(values: ArrayRef) -> Self {
        Self { values }
    }

    pub fn doc_count(&self) -> usize {
        self.values.len()
    }

    pub fn array(&self) -> &ArrayRef {
        &self.values
    }
}
