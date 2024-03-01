use arrow::array::ArrayRef;

use crate::DocId;

use super::ColumnPersistentSegmentData;

pub trait ColumnMerger {
    fn merge(
        &self,
        segments: &[&ColumnPersistentSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    ) -> ArrayRef;
}
