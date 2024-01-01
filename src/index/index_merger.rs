use std::path::Path;

use crate::{schema::Index, DocId};

use super::IndexSegmentData;

pub trait IndexMerger {
    fn merge(
        &self,
        directory: &Path,
        index: &Index,
        segments: &[&dyn IndexSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    );
}
