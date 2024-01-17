use std::{path::Path, sync::Arc};

use crate::{schema::Index, DocId};

use super::IndexSegmentData;

pub trait IndexMerger {
    fn merge(
        &self,
        directory: &Path,
        index: &Index,
        segments: &[&Arc<dyn IndexSegmentData>],
        docid_mappings: &[Vec<Option<DocId>>],
    );
}
