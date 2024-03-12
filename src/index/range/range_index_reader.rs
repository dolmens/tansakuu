use crate::{schema::Index, table::TableData, index::IndexReader};

use super::RangeIndexBuildingSegmentReader;

pub struct RangeIndexReader {
    // persistent_segments: Vec<>
    building_segments: Vec<RangeIndexBuildingSegmentReader>,
}

impl RangeIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut building_segments = vec![];
        for segment in table_data.building_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data().index_data(index.name()).unwrap();
            let range_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader =
                RangeIndexBuildingSegmentReader::new(meta.base_docid(), range_index_data);
            building_segments.push(index_segment_reader);
        }

        Self { building_segments }
    }
}

impl IndexReader for RangeIndexReader {
    fn lookup<'a>(&'a self, key: &crate::query::Term) -> Option<Box<dyn crate::index::PostingIterator + 'a>> {
        unimplemented!()
    }
}
