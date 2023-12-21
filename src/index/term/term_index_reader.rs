use crate::{
    index::{
        term::{
            BufferedPostingIterator, TermIndexBuildingSegmentData, TermIndexBuildingSegmentReader,
        },
        IndexReader,
    },
    schema::Index,
    table::TableData,
};

pub struct TermIndexReader {
    building_segments: Vec<TermIndexBuildingSegmentReader>,
}

impl TermIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut building_segments = vec![];
        for building_segment in table_data
            .dumping_segments()
            .chain(table_data.building_segments())
        {
            let index_data = building_segment
                .index_data()
                .index_data(index.name())
                .unwrap();
            let term_index_data = index_data
                .clone()
                .downcast_arc::<TermIndexBuildingSegmentData>()
                .ok()
                .unwrap();
            let index_segment_reader = TermIndexBuildingSegmentReader::new(term_index_data);
            building_segments.push(index_segment_reader);
        }

        Self { building_segments }
    }
}

impl IndexReader for TermIndexReader {
    fn lookup(&self, key: &str) -> Option<Box<dyn crate::index::PostingIterator>> {
        let mut segment_postings = vec![];
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
