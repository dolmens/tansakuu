use crate::{
    index::{
        inverted_index::{MultiPostingIterator, TokenHasher},
        IndexReader,
    },
    postings::PostingFormat,
    schema::Index,
    table::TableData,
};

use super::{
    RangeIndexBuildingSegmentReader, RangeIndexPersistentSegmentReader, RangeQueryEncoder,
    RangeValueEncoder,
};

pub struct RangeIndexReader {
    persistent_segments: Vec<RangeIndexPersistentSegmentReader>,
    building_segments: Vec<RangeIndexBuildingSegmentReader>,
    range_value_encoder: RangeValueEncoder,
    range_query_encoder: RangeQueryEncoder,
    token_hasher: TokenHasher,
}

impl RangeIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data(index.name());
            let range_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader =
                RangeIndexPersistentSegmentReader::new(meta.base_docid(), range_index_data);
            persistent_segments.push(index_segment_reader);
        }

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

        Self {
            persistent_segments,
            building_segments,
            range_value_encoder: RangeValueEncoder::default(),
            range_query_encoder: RangeQueryEncoder::default(),
            token_hasher: TokenHasher::default(),
        }
    }
}

impl IndexReader for RangeIndexReader {
    fn lookup<'a>(
        &'a self,
        term: &crate::query::Term,
    ) -> Option<Box<dyn crate::index::PostingIterator + 'a>> {
        let (left, right) = self.range_query_encoder.decode(term.keyword());
        let (bottom_ranges, higher_ranges) = self.range_value_encoder.searching_ranges(left, right);
        let bottom_keys: Vec<_> = bottom_ranges
            .into_iter()
            .flat_map(|(l, r)| (l..=r).map(|v| self.token_hasher.hash_bytes(&v.to_le_bytes())))
            .collect();
        let higher_keys: Vec<_> = higher_ranges
            .into_iter()
            .flat_map(|(l, r)| (l..=r).map(|v| self.token_hasher.hash_bytes(&v.to_le_bytes())))
            .collect();

        let mut postings = vec![];
        for segment_reader in &self.persistent_segments {
            if let Some(segment_posting) = segment_reader.lookup(&bottom_keys, &higher_keys) {
                postings.push(segment_posting);
            }
        }
        for segment_reader in &self.building_segments {
            if let Some(segment_posting) = segment_reader.lookup(&bottom_keys, &higher_keys) {
                postings.push(segment_posting);
            }
        }
        Some(Box::new(MultiPostingIterator::new(
            PostingFormat::default(),
            postings,
        )))
    }
}
