use std::path::Path;

use crate::schema::Index;

use super::IndexSegmentData;

pub trait IndexMerger {
    fn merge(
        &self,
        directory: &Path,
        index: &Index,
        segments: &[&dyn IndexSegmentData],
        doc_counts: &[usize],
    );
}
