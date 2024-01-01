use std::path::Path;

use crate::{schema::Field, DocId};

use super::ColumnSegmentData;

pub trait ColumnMerger {
    fn merge(
        &self,
        directory: &Path,
        field: &Field,
        segments: &[&dyn ColumnSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    );
}
