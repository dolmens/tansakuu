use crate::{index::IndexReader, schema::Index, table::TableData, DocId};

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
        for segment_info in table_data.segments() {
            let meta_info = segment_info.meta_info();
            let segment = segment_info.segment();
            let index_data = segment.index_data(index.name());
            let primary_key_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader =
                PrimaryKeyIndexSegmentReader::new(meta_info.clone(), primary_key_index_data);
            segments.push(index_segment_reader);
        }

        let mut building_segments = vec![];
        for segment_info in table_data.building_segments() {
            let meta_info = segment_info.meta_info();
            let segment = segment_info.segment();
            let index_data = segment.index_data().index_data(index.name()).unwrap();
            let primary_key_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let primary_key_segment_reader = PrimaryKeyIndexBuildingSegmentReader::new(
                meta_info.clone(),
                primary_key_index_data,
            );
            building_segments.push(primary_key_segment_reader);
        }

        Self {
            segments,
            building_segments,
        }
    }

    pub fn get(&self, primary_key: &str) -> Option<DocId> {
        for segment in self.building_segments.iter().rev() {
            if let Some(docid) = segment.lookup(primary_key) {
                return Some(docid);
            }
        }

        for segment in self.segments.iter().rev() {
            if let Some(docid) = segment.lookup(primary_key) {
                return Some(docid);
            }
        }

        None
    }
}

impl IndexReader for PrimaryKeyIndexReader {
    fn lookup(&self, key: &str) -> Option<Box<dyn crate::index::PostingIterator>> {
        self.get(key).map(|docid| {
            Box::new(PrimaryKeyPostingIterator::new(docid))
                as Box<dyn crate::index::PostingIterator>
        })
    }
}
