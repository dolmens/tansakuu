use std::sync::Arc;

use crate::{
    index::{IndexSegmentData, IndexSerializer},
    schema::IndexRef,
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
        _doc_count: usize,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    ) {
        let index_name = index.name();

        let inverted_index_data = index_data
            .clone()
            .downcast_arc::<InvertedIndexBuildingSegmentData>()
            .ok()
            .unwrap();

        let posting_serializer = InvertedIndexPostingSerializer::default();
        posting_serializer.serialize(
            index_name,
            inverted_index_data.posting_format,
            &inverted_index_data.postings,
            directory,
            index_path,
            docid_mapping,
        );
    }
}
