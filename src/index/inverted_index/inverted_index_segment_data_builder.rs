use tantivy_common::{HasLen, OwnedBytes};

use crate::{
    index::IndexSegmentDataBuilder,
    postings::{PostingFormat, TermDict},
    schema::{Index, IndexType},
    Directory,
};

use super::InvertedIndexPersistentSegmentData;

pub struct InvertedIndexSegmentDataBuilder {}

impl InvertedIndexSegmentDataBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl IndexSegmentDataBuilder for InvertedIndexSegmentDataBuilder {
    fn build(
        &self,
        index: &Index,
        directory: &dyn Directory,
        index_directory: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let posting_format = if let IndexType::Text(text_index_options) = index.index_type() {
            PostingFormat::builder()
                .with_text_index_options(text_index_options)
                .build()
        } else {
            PostingFormat::default()
        };
        let index_name = index.name();
        let dict_path = index_directory.join(index_name.to_string() + ".dict");
        let dict_data = directory.open_read(&dict_path).unwrap();
        let term_dict = TermDict::open(dict_data).unwrap();

        let skip_list_path = index_directory.join(index_name.to_string() + ".skiplist");
        let skip_list_slice = directory.open_read(&skip_list_path).unwrap();
        let skip_list_data = if skip_list_slice.len() > 0 {
            skip_list_slice.read_bytes().unwrap()
        } else {
            OwnedBytes::empty()
        };

        let posting_path = index_directory.join(index_name.to_string() + ".posting");
        let posting_data = directory.open_read(&posting_path).unwrap();
        let posting_data = posting_data.read_bytes().unwrap();

        let position_skip_list_data = if posting_format.has_position_list() {
            let position_skip_list_path =
                index_directory.join(index_name.to_string() + ".positions.skiplist");
            let position_skip_list_slice = directory.open_read(&position_skip_list_path).unwrap();
            position_skip_list_slice.read_bytes().unwrap()
        } else {
            OwnedBytes::empty()
        };
        let position_list_data = if posting_format.has_position_list() {
            let position_list_path = index_directory.join(index_name.to_string() + ".positions");
            let position_list_slice = directory.open_read(&position_list_path).unwrap();
            position_list_slice.read_bytes().unwrap()
        } else {
            OwnedBytes::empty()
        };

        Box::new(InvertedIndexPersistentSegmentData::new(
            posting_format,
            term_dict,
            skip_list_data,
            posting_data,
            position_skip_list_data,
            position_list_data,
        ))
    }
}
