use crate::{
    index::{
        term::{BufferedPostingIterator, TermIndexBuildingSegmentReader, TermIndexSegmentReader},
        IndexReader,
    },
    schema::Index,
    table::{TableData, TableDataSnapshot},
    DocId,
};

pub struct TermIndexReader {
    segments: Vec<TermIndexSegmentReader>,
    building_segments: Vec<TermIndexBuildingSegmentReader>,
}

impl TermIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut segments = vec![];
        for segment in table_data.segments() {
            let index_data = segment.index_data(index.name());
            let term_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader = TermIndexSegmentReader::new(term_index_data);
            segments.push(index_segment_reader);
        }

        let mut building_segments = vec![];
        for building_segment in table_data.building_segments() {
            let index_data = building_segment
                .index_data()
                .index_data(index.name())
                .unwrap();
            let term_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader = TermIndexBuildingSegmentReader::new(term_index_data);
            building_segments.push(index_segment_reader);
        }

        Self {
            segments,
            building_segments,
        }
    }
}

impl IndexReader for TermIndexReader {
    fn lookup(
        &self,
        key: &str,
        data_snapshot: &TableDataSnapshot,
    ) -> Option<Box<dyn crate::index::PostingIterator>> {
        let mut segment_postings = vec![];
        let mut segment_cursor = 0;
        for segment_reader in &self.segments {
            let mut segment_posting = segment_reader.segment_posting(key);
            if !segment_posting.is_empty() {
                segment_posting.set_base_docid(data_snapshot.segments[segment_cursor] as DocId);
                segment_postings.push(segment_posting);
            }
            segment_cursor += 1;
        }
        for segment_reader in &self.building_segments {
            let mut segment_posting = segment_reader.segment_posting(key);
            if !segment_posting.is_empty() {
                segment_posting.set_base_docid(data_snapshot.segments[segment_cursor] as DocId);
                segment_postings.push(segment_posting);
            }
            segment_cursor += 1;
        }
        if !segment_postings.is_empty() {
            Some(Box::new(BufferedPostingIterator::new(segment_postings)))
        } else {
            None
        }
    }
}
