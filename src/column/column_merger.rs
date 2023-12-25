use std::path::Path;

use crate::schema::Field;

use super::ColumnSegmentData;

pub trait ColumnMerger {
    fn merge(
        &self,
        directory: &Path,
        field: &Field,
        segments: &[&dyn ColumnSegmentData],
        doc_counts: &[usize],
    );
}
