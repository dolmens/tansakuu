use crate::{index::IndexSegmentDataBuilder, schema::Index, Directory};

use super::{InvertedIndexPersistentSegmentData, PostingDataLoader};

#[derive(Default)]
pub struct InvertedIndexSegmentDataBuilder {}

impl IndexSegmentDataBuilder for InvertedIndexSegmentDataBuilder {
    fn build(
        &self,
        index: &Index,
        directory: &dyn Directory,
        index_path: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let posting_data_loader = PostingDataLoader::default();
        let posting_data = posting_data_loader
            .load(index.name(), directory, index_path)
            .unwrap();

        Box::new(InvertedIndexPersistentSegmentData::new(posting_data))
    }
}
