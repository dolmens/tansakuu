use std::sync::Arc;

use crate::{
    index::{inverted_index::InvertedIndexPostingSerializer, IndexSegmentData, IndexSerializer},
    postings::PostingFormat,
    schema::IndexRef,
};

use super::RangeIndexBuildingSegmentData;

#[derive(Default)]
pub struct RangeIndexSerializer {}

impl IndexSerializer for RangeIndexSerializer {
    fn serialize(
        &self,
        index: &IndexRef,
        index_data: &Arc<dyn IndexSegmentData>,
        directory: &dyn crate::Directory,
        index_path: &std::path::Path,
        docid_mapping: Option<&Vec<Option<crate::DocId>>>,
    ) {
        let range_index_data = index_data
            .clone()
            .downcast_arc::<RangeIndexBuildingSegmentData>()
            .ok()
            .unwrap();
        let posting_serializer = InvertedIndexPostingSerializer::default();
        let posting_format = PostingFormat::default();
        let index_path = index_path.join(index.name());
        let bottom_postings = &range_index_data.bottom_postings;
        posting_serializer.serialize(
            "bottom",
            &posting_format,
            bottom_postings,
            directory,
            &index_path,
            docid_mapping,
        );
        let higher_postings = &range_index_data.higher_postings;
        posting_serializer.serialize(
            "higher",
            &posting_format,
            higher_postings,
            directory,
            &index_path,
            docid_mapping,
        );
    }
}
