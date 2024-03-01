use arrow::array::ArrayRef;

pub struct ColumnPersistentSegmentData {
    doc_count: usize,
    values: ArrayRef,
}

impl ColumnPersistentSegmentData {
    pub fn new(doc_count: usize, values: ArrayRef) -> Self {
        Self { doc_count, values }
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    pub fn array(&self) -> &ArrayRef {
        &self.values
    }
}
