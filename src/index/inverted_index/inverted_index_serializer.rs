use std::sync::Arc;

use crate::{
    index::{IndexSegmentData, IndexSerializer},
    postings::PostingFormat,
    schema::{IndexRef, IndexType},
    Directory, DocId,
};

use super::{InvertedIndexBuildingSegmentData, InvertedIndexPostingSerializer};

#[derive(Default)]
pub struct InvertedIndexSerializer {}

impl IndexSerializer for InvertedIndexSerializer {
    fn serialize(
        &self,
        index: &IndexRef,
        index_data: &Arc<dyn IndexSegmentData>,
        directory: &dyn Directory,
        index_path: &std::path::Path,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    ) {
        let posting_format = if let IndexType::Text(text_index_options) = index.index_type() {
            PostingFormat::builder()
                .with_index_options(text_index_options)
                .build()
        } else {
            PostingFormat::builder().build()
        };

        let index_name = index.name();

        let inverted_index_data = index_data
            .clone()
            .downcast_arc::<InvertedIndexBuildingSegmentData>()
            .ok()
            .unwrap();

        let posting_serializer = InvertedIndexPostingSerializer::default();
        posting_serializer.serialize(
            index_name,
            &posting_format,
            &inverted_index_data.postings,
            directory,
            index_path,
            docid_mapping,
        );
    }
}
