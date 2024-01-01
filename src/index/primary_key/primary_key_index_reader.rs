use crate::{index::IndexReader, schema::Index, table::TableData, DocId};

use super::{
    PrimaryKeyIndexBuildingSegmentReader, PrimaryKeyIndexPersistentSegmentReader,
    PrimaryKeyPostingIterator,
};

pub struct PrimaryKeyIndexReader {
    persistent_segments: Vec<PrimaryKeyIndexPersistentSegmentReader>,
    building_segments: Vec<PrimaryKeyIndexBuildingSegmentReader>,
}

impl PrimaryKeyIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data(index.name());
            let primary_key_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let primary_key_segment_reader =
                PrimaryKeyIndexPersistentSegmentReader::new(meta.clone(), primary_key_index_data);
            persistent_segments.push(primary_key_segment_reader);
        }

        let mut building_segments = vec![];
        for segment in table_data.building_segments() {
            let meta = segment.meta();
            let segment_data = segment.segment();
            let index_data = segment_data.index_data().index_data(index.name()).unwrap();
            let primary_key_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let primary_key_segment_reader =
                PrimaryKeyIndexBuildingSegmentReader::new(meta.clone(), primary_key_index_data);
            building_segments.push(primary_key_segment_reader);
        }

        Self {
            persistent_segments,
            building_segments,
        }
    }

    pub fn get(&self, primary_key: &str) -> Option<DocId> {
        for segment in self.building_segments.iter().rev() {
            if let Some(docid) = segment.lookup(primary_key) {
                return Some(docid);
            }
        }

        for segment in self.persistent_segments.iter().rev() {
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
