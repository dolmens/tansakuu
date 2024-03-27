use std::sync::Arc;

use crate::{index::IndexMerger, Directory, DocId};

use super::{InvertedIndexPersistentSegmentData, InvertedIndexPostingMerger};

#[derive(Default)]
pub struct InvertedIndexMerger {}

impl IndexMerger for InvertedIndexMerger {
    fn merge(
        &self,
        directory: &dyn Directory,
        index_path: &std::path::Path,
        index: &crate::schema::Index,
        _total_doc_count: usize,
        segments: &[&Arc<dyn crate::index::IndexSegmentData>],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        let posting_datas: Vec<_> = segments
            .iter()
            .map(|&seg| {
                seg.downcast_ref::<InvertedIndexPersistentSegmentData>()
                    .unwrap()
            })
            .map(|seg| &seg.posting_data)
            .collect();
        let posting_merger = InvertedIndexPostingMerger::default();
        posting_merger.merge(
            directory,
            index_path,
            index.name(),
            &posting_datas,
            docid_mappings,
        );
    }
}
