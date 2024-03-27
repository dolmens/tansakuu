use std::{path::Path, sync::Arc};

use crate::{schema::Index, Directory, DocId};

use super::IndexSegmentData;

pub trait IndexMerger {
    fn merge(
        &self,
        directory: &dyn Directory,
        index_path: &Path,
        index: &Index,
        total_doc_count: usize,
        segments: &[&Arc<dyn IndexSegmentData>],
        docid_mappings: &[Vec<Option<DocId>>],
    );
}
