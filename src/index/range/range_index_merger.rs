use crate::index::IndexMerger;

#[derive(Default)]
pub struct RangeIndexMerger {}

impl IndexMerger for RangeIndexMerger {
    fn merge(
        &self,
        directory: &dyn crate::Directory,
        index_path: &std::path::Path,
        index: &crate::schema::Index,
        segments: &[&std::sync::Arc<dyn crate::index::IndexSegmentData>],
        docid_mappings: &[Vec<Option<crate::DocId>>],
    ) {
        unimplemented!()
    }
}
