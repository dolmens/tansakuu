pub trait ColumnSegmentReader {
    fn doc_count(&self) -> usize;
}
