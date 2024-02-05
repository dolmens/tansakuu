use crate::{
    index::{
        inverted_index::{
            BufferedPostingIterator, InvertedIndexBuildingSegmentReader,
            InvertedIndexPersistentSegmentReader,
        },
        IndexReader,
    },
    postings::PostingFormat,
    query::Term,
    schema::{Index, IndexType},
    table::TableData,
    util::hash::hash_string_64,
};

pub struct InvertedIndexReader {
    posting_format: PostingFormat,
    persistent_segments: Vec<InvertedIndexPersistentSegmentReader>,
    building_segments: Vec<InvertedIndexBuildingSegmentReader>,
}

impl InvertedIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let posting_format = if let IndexType::Text(text_index_options) = index.index_type() {
            PostingFormat::builder()
                .with_text_index_options(text_index_options)
                .build()
        } else {
            PostingFormat::default()
        };

        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data(index.name());
            let inverted_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader =
                InvertedIndexPersistentSegmentReader::new(meta.base_docid(), inverted_index_data);
            persistent_segments.push(index_segment_reader);
        }

        let mut building_segments = vec![];
        for segment in table_data.building_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data().index_data(index.name()).unwrap();
            let inverted_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader =
                InvertedIndexBuildingSegmentReader::new(meta.base_docid(), inverted_index_data);
            building_segments.push(index_segment_reader);
        }

        Self {
            posting_format,
            persistent_segments,
            building_segments,
        }
    }
}

impl IndexReader for InvertedIndexReader {
    fn lookup<'a>(&'a self, term: &Term) -> Option<Box<dyn crate::index::PostingIterator + 'a>> {
        let hashkey = hash_string_64(term.keyword());
        let mut segment_postings = vec![];
        for segment_reader in &self.persistent_segments {
            if let Some(segment_posting) = segment_reader.segment_posting(hashkey) {
                segment_postings.push(segment_posting);
            }
        }
        for segment_reader in &self.building_segments {
            if let Some(segment_posting) = segment_reader.segment_posting(hashkey) {
                segment_postings.push(segment_posting);
            }
        }
        if !segment_postings.is_empty() {
            Some(Box::new(BufferedPostingIterator::new(
                &self.posting_format,
                segment_postings,
            )))
        } else {
            None
        }
    }
}
