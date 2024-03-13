use crate::{
    index::{inverted_index::PostingDataLoader, IndexSegmentDataBuilder},
    postings::PostingFormat,
};

use super::range_index_persistent_segment_data::RangeIndexPersistentSegmentData;

#[derive(Default)]
pub struct RangeIndexSegmentDataBuilder {}

impl IndexSegmentDataBuilder for RangeIndexSegmentDataBuilder {
    fn build(
        &self,
        index: &crate::schema::Index,
        directory: &dyn crate::Directory,
        index_path: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let posting_format = PostingFormat::default();
        let posting_data_loader = PostingDataLoader::default();
        let bottom_index_path = index_path.join("bottom");
        let bottom_posting_data = posting_data_loader
            .load("botom", &posting_format, directory, &bottom_index_path)
            .unwrap();
        let higher_index_path = index_path.join("higher");
        let higher_posting_data = posting_data_loader
            .load("higher", &posting_format, directory, &higher_index_path)
            .unwrap();

        Box::new(RangeIndexPersistentSegmentData {
            bottom_posting_data,
            higher_posting_data,
        })
    }
}
