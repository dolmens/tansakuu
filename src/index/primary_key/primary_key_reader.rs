use crate::{
    index::IndexReader, query::Term, schema::Index, table::TableData, util::hash::hash_string_64,
    DocId,
};

use super::{
    PrimaryKeyBuildingSegmentReader, PrimaryKeyPersistentSegmentReader, PrimaryKeyPostingIterator,
};

pub struct PrimaryKeyReader {
    persistent_segments: Vec<PrimaryKeyPersistentSegmentReader>,
    building_segments: Vec<PrimaryKeyBuildingSegmentReader>,
}

impl PrimaryKeyReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data(index.name());
            let primary_key_data = index_data.clone().downcast_arc().ok().unwrap();
            let primary_key_segment_reader =
                PrimaryKeyPersistentSegmentReader::new(meta.clone(), primary_key_data);
            persistent_segments.push(primary_key_segment_reader);
        }

        let mut building_segments = vec![];
        for segment in table_data.building_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data().index_data(index.name()).unwrap();
            let primary_key_data = index_data.clone().downcast_arc().ok().unwrap();
            let primary_key_segment_reader =
                PrimaryKeyBuildingSegmentReader::new(meta.clone(), primary_key_data);
            building_segments.push(primary_key_segment_reader);
        }

        Self {
            persistent_segments,
            building_segments,
        }
    }

    pub fn get_by_hashkey(&self, hashkey: u64) -> Option<DocId> {
        for segment in self.building_segments.iter().rev() {
            if let Some(docid) = segment.get_by_hashkey(hashkey) {
                return Some(docid);
            }
        }

        for segment in self.persistent_segments.iter().rev() {
            if let Some(docid) = segment.get_by_hashkey(hashkey) {
                return Some(docid);
            }
        }

        None
    }

    pub fn lookup(&self, term: &Term) -> Option<DocId> {
        let hashkey = hash_string_64(term.keyword());
        self.get_by_hashkey(hashkey)
    }
}

impl IndexReader for PrimaryKeyReader {
    fn lookup(&self, term: &Term) -> Option<Box<dyn crate::index::PostingIterator>> {
        let hashkey = hash_string_64(term.keyword());
        self.get_by_hashkey(hashkey).map(|docid| {
            Box::new(PrimaryKeyPostingIterator::new(docid))
                as Box<dyn crate::index::PostingIterator>
        })
    }
}
