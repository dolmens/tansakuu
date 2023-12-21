use crate::{index::IndexReader, schema::Index, table::TableData, END_DOCID};

use super::{
    unique_key_posting_iterator::UniqueKeyPostingIterator, UniqueKeyIndexBuildingSegmentData,
    UniqueKeyIndexBuildingSegmentReader,
};

pub struct UniqueKeyIndexReader {
    building_segments: Vec<UniqueKeyIndexBuildingSegmentReader>,
}

impl UniqueKeyIndexReader {
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
            let unique_key_index_data = index_data
                .clone()
                .downcast_arc::<UniqueKeyIndexBuildingSegmentData>()
                .ok()
                .unwrap();
            let unique_key_segment_reader =
                UniqueKeyIndexBuildingSegmentReader::new(unique_key_index_data);
            building_segments.push(unique_key_segment_reader);
        }

        Self { building_segments }
    }
}

impl IndexReader for UniqueKeyIndexReader {
    fn lookup(&self, key: &str) -> Option<Box<dyn crate::index::PostingIterator>> {
        for segment_reader in self.building_segments.iter().rev() {
            let docid = segment_reader.lookup(key);
            if docid != END_DOCID {
                return Some(Box::new(UniqueKeyPostingIterator::new(docid)));
            }
        }
        None
    }
}
