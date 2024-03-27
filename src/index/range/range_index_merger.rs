use crate::index::{
    inverted_index::InvertedIndexPostingMerger,
    range::range_index_persistent_segment_data::RangeIndexPersistentSegmentData, IndexMerger,
};

#[derive(Default)]
pub struct RangeIndexMerger {}

impl IndexMerger for RangeIndexMerger {
    fn merge(
        &self,
        directory: &dyn crate::Directory,
        index_path: &std::path::Path,
        index: &crate::schema::Index,
        _total_doc_count: usize,
        segments: &[&std::sync::Arc<dyn crate::index::IndexSegmentData>],
        docid_mappings: &[Vec<Option<crate::DocId>>],
    ) {
        let mut bottom_posting_datas = vec![];
        let mut higher_posting_datas = vec![];
        for &segment in segments {
            let range_index_data = segment
                .downcast_ref::<RangeIndexPersistentSegmentData>()
                .unwrap();
            bottom_posting_datas.push(&range_index_data.bottom_posting_data);
            higher_posting_datas.push(&range_index_data.higher_posting_data);
        }

        let posting_merger = InvertedIndexPostingMerger::default();
        let index_path = index_path.join(index.name());
        posting_merger.merge(
            directory,
            &index_path,
            "bottom",
            &bottom_posting_datas,
            docid_mappings,
        );
        posting_merger.merge(
            directory,
            &index_path,
            "higher",
            &higher_posting_datas,
            docid_mappings,
        );
    }
}
