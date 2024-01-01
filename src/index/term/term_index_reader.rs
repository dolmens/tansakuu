use crate::{
    index::{
        term::{
            BufferedPostingIterator, TermIndexBuildingSegmentReader,
            TermIndexPersistentSegmentReader,
        },
        IndexReader,
    },
    schema::Index,
    table::TableData,
};

pub struct TermIndexReader {
    persistent_segments: Vec<TermIndexPersistentSegmentReader>,
    building_segments: Vec<TermIndexBuildingSegmentReader>,
}

impl TermIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let meta = segment.meta();
            let segment_data = segment.segment();
            let index_data = segment_data.index_data(index.name());
            let term_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader =
                TermIndexPersistentSegmentReader::new(meta.base_docid(), term_index_data);
            persistent_segments.push(index_segment_reader);
        }

        let mut building_segments = vec![];
        for segment in table_data.building_segments() {
            let meta = segment.meta();
            let segment_data = segment.segment();
            let index_data = segment_data.index_data().index_data(index.name()).unwrap();
            let term_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader =
                TermIndexBuildingSegmentReader::new(meta.base_docid(), term_index_data);
            building_segments.push(index_segment_reader);
        }

        Self {
            persistent_segments,
            building_segments,
        }
    }
}

impl IndexReader for TermIndexReader {
    fn lookup(&self, key: &str) -> Option<Box<dyn crate::index::PostingIterator>> {
        let mut segment_postings = vec![];
        for segment_reader in &self.persistent_segments {
            let segment_posting = segment_reader.segment_posting(key);
            if !segment_posting.is_empty() {
                segment_postings.push(segment_posting);
            }
        }
        for segment_reader in &self.building_segments {
            let segment_posting = segment_reader.segment_posting(key);
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
