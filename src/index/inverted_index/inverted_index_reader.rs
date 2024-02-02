use crate::{
    index::{
        inverted_index::{
            BufferedPostingIterator, InvertedIndexBuildingSegmentReader,
            InvertedIndexPersistentSegmentReader,
        },
        IndexReader,
    },
    query::Term,
    schema::Index,
    table::TableData,
    util::hash::hash_string_64,
};

pub struct InvertedIndexReader {
    persistent_segments: Vec<InvertedIndexPersistentSegmentReader>,
    building_segments: Vec<InvertedIndexBuildingSegmentReader>,
}

impl InvertedIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
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
            persistent_segments,
            building_segments,
        }
    }
}

impl IndexReader for InvertedIndexReader {
    fn lookup(&self, term: &Term) -> Option<Box<dyn crate::index::PostingIterator>> {
        let mut segment_postings = vec![];
        let hashkey = hash_string_64(term.keyword());
        for segment_reader in &self.persistent_segments {
            let segment_posting = segment_reader.segment_posting(hashkey);
            if !segment_posting.is_empty() {
                segment_postings.push(segment_posting);
            }
        }
        for segment_reader in &self.building_segments {
            let segment_posting = segment_reader.segment_posting(hashkey);
            if !segment_posting.is_empty() {
                segment_postings.push(segment_posting);
            }
        }
        if !segment_postings.is_empty() {
            Some(Box::new(BufferedPostingIterator::new(segment_postings)))
        } else {
            None
        }
    }
}
