use crate::{
    index::IndexReader,
    schema::Index,
    table::{TableData, TableDataSnapshot},
};

use super::{
    PrimaryKeyIndexBuildingSegmentReader, PrimaryKeyIndexSegmentReader, PrimaryKeyPostingIterator,
};

pub struct PrimaryKeyIndexReader {
    segments: Vec<PrimaryKeyIndexSegmentReader>,
    building_segments: Vec<PrimaryKeyIndexBuildingSegmentReader>,
}

impl PrimaryKeyIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut segments = vec![];
        for segment in table_data.segments() {
            let index_data = segment.index_data(index.name());
            let primary_key_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader = PrimaryKeyIndexSegmentReader::new(primary_key_index_data);
            segments.push(index_segment_reader);
        }

        let mut building_segments = vec![];
        for building_segment in table_data.building_segments() {
            let index_data = building_segment
                .index_data()
                .index_data(index.name())
                .unwrap();
            let primary_key_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let primary_key_segment_reader =
                PrimaryKeyIndexBuildingSegmentReader::new(primary_key_index_data);
            building_segments.push(primary_key_segment_reader);
        }

        Self {
            segments,
            building_segments,
        }
    }
}

impl IndexReader for PrimaryKeyIndexReader {
    fn lookup(
        &self,
        key: &str,
        data_snapshot: &TableDataSnapshot,
    ) -> Option<Box<dyn crate::index::PostingIterator>> {
        let _ = data_snapshot;
        for segment_reader in self.building_segments.iter().rev() {
            let docid = segment_reader.lookup(key);
            if let Some(docid) = docid {
                return Some(Box::new(PrimaryKeyPostingIterator::new(docid)));
            }
        }
        for segment_reader in self.segments.iter().rev() {
            let docid = segment_reader.lookup(key);
            if let Some(docid) = docid {
                return Some(Box::new(PrimaryKeyPostingIterator::new(docid)));
            }
        }
        None
    }
}
