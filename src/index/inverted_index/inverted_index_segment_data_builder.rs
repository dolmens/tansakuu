use crate::{
    index::IndexSegmentDataBuilder,
    postings::PostingFormat,
    schema::{Index, IndexType},
    Directory,
};

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
        let posting_format = if let IndexType::Text(text_index_options) = index.index_type() {
            PostingFormat::builder()
                .with_index_options(text_index_options)
                .build()
        } else {
            PostingFormat::default()
        };
        let posting_data_loader = PostingDataLoader::default();
        let posting_data = posting_data_loader
            .load(index.name(), posting_format, directory, index_path)
            .unwrap();

        Box::new(InvertedIndexPersistentSegmentData::new(posting_data))
    }
}
