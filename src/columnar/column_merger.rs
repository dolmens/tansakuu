use std::path::Path;

use crate::{schema::Field, Directory, DocId};

use super::ColumnSegmentData;

pub trait ColumnMerger {
    fn merge(
        &self,
        directory: &dyn Directory,
        segment_directory: &Path,
        field: &Field,
        segments: &[&dyn ColumnSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    );
}
