use std::{fs::File, sync::Arc};

use tantivy_common::{
    file_slice::{FileSlice, WrapFile},
    HasLen, OwnedBytes,
};

use crate::{index::IndexSegmentDataBuilder, postings::TermDict, schema::Index};

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
        path: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let index_name = index.name();
        let dict_path = path.join(index_name.to_string() + ".dict");
        let dict_file = File::open(dict_path).unwrap();
        let dict_data = FileSlice::new(Arc::new(WrapFile::new(dict_file).unwrap()));
        let term_dict = TermDict::open(dict_data).unwrap();

        let skip_path = path.join(index_name.to_string() + ".skiplist");
        let skip_file = File::open(skip_path).unwrap();
        let skip_data = FileSlice::new(Arc::new(WrapFile::new(skip_file).unwrap()));
        let skip_data = if skip_data.len() > 0 {
            skip_data.read_bytes().unwrap()
        } else {
            OwnedBytes::empty()
        };

        let posting_path = path.join(index_name.to_string() + ".posting");
        let posting_file = File::open(posting_path).unwrap();
        let posting_data = FileSlice::new(Arc::new(WrapFile::new(posting_file).unwrap()));
        let posting_data = posting_data.read_bytes().unwrap();

        Box::new(InvertedIndexPersistentSegmentData::new(
            term_dict,
            skip_data,
            posting_data,
        ))
    }
}
