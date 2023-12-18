use crate::{index::IndexReader, schema::Index, table::TableData, END_DOCID};

use super::{
    unique_key_posting_iterator::UniqueKeyPostingIterator, UniqueKeyIndexSegmentData,
    UniqueKeyIndexSegmentReader,
};

pub struct UniqueKeyIndexReader {
    segment_readers: Vec<UniqueKeyIndexSegmentReader>,
}

impl UniqueKeyIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut segment_readers = vec![];
        for building_segment in table_data.building_segments() {
            let index_data = building_segment.index_data(index.name());
            let unique_key_index_data = index_data
                .clone()
                .downcast_arc::<UniqueKeyIndexSegmentData>()
                .ok()
                .unwrap();
            let unique_key_segment_reader = UniqueKeyIndexSegmentReader::new(unique_key_index_data);
            segment_readers.push(unique_key_segment_reader);
        }

        Self { segment_readers }
    }
}

impl IndexReader for UniqueKeyIndexReader {
    fn lookup(&self, key: &str) -> Option<Box<dyn crate::index::PostingIterator>> {
        for segment_reader in self.segment_readers.iter().rev() {
            let docid = segment_reader.lookup(key);
            if docid != END_DOCID {
                return Some(Box::new(UniqueKeyPostingIterator::new(docid)));
            }
        }
        None
    }
}
