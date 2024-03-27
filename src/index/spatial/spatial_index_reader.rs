use crate::{
    index::{inverted_index::MultiPostingIterator, IndexReader},
    postings::PostingFormat,
    schema::Index,
    table::TableData,
};

use super::{
    geohash::{
        geohash_embed_step, geohash_encode, geohash_estimate_steps_by_radius, geohash_neighbors,
    },
    SpatialIndexBuildingSegmentReader, SpatialIndexPersistentSegmentReader, SpatialQueryParser,
};

pub struct SpatialIndexReader {
    persistent_segments: Vec<SpatialIndexPersistentSegmentReader>,
    building_segments: Vec<SpatialIndexBuildingSegmentReader>,
}

impl SpatialIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data(index.name()).unwrap();
            let spatial_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader = SpatialIndexPersistentSegmentReader::new(
                meta.base_docid(),
                meta.doc_count(),
                spatial_index_data,
            );
            persistent_segments.push(index_segment_reader);
        }

        let mut building_segments = vec![];
        for segment in table_data.building_segments() {
            let meta = segment.meta();
            let data = segment.data();
            let index_data = data.index_data().index_data(index.name()).unwrap();
            let spatial_index_data = index_data.clone().downcast_arc().ok().unwrap();
            let index_segment_reader = SpatialIndexBuildingSegmentReader::new(
                meta.base_docid(),
                data.doc_count().clone(),
                spatial_index_data,
            );
            building_segments.push(index_segment_reader);
        }

        Self {
            persistent_segments,
            building_segments,
        }
    }
}

impl IndexReader for SpatialIndexReader {
    fn lookup<'a>(
        &'a self,
        term: &crate::query::Term,
    ) -> Option<Box<dyn crate::index::PostingIterator + 'a>> {
        let query_parser = SpatialQueryParser::default();
        let term = match query_parser.parse(term.keyword()) {
            Some(term) => term,
            None => {
                return None;
            }
        };
        let step = geohash_estimate_steps_by_radius(term.distance, term.latitude);
        let hash = match geohash_encode(term.longitude, term.latitude, step) {
            Ok(hash) => hash,
            Err(_) => {
                return None;
            }
        };
        let neighbors = match geohash_neighbors(hash, step) {
            Ok(neighbors) => neighbors,
            Err(_) => {
                return None;
            }
        };
        let neighbors_vec: Vec<_> = neighbors.into();

        let mut hashkeys = vec![hash];
        hashkeys.extend(neighbors_vec);
        let hashkeys: Vec<_> = hashkeys
            .iter()
            .map(|&h| geohash_embed_step(h, step).unwrap())
            .collect();

        let mut postings = vec![];
        for segment_reader in &self.persistent_segments {
            if let Some(segment_posting) = segment_reader.lookup(&hashkeys) {
                postings.push(segment_posting);
            }
        }
        for segment_reader in &self.building_segments {
            if let Some(segment_posting) = segment_reader.lookup(&hashkeys) {
                postings.push(segment_posting);
            }
        }

        Some(Box::new(MultiPostingIterator::new(
            PostingFormat::default(),
            postings,
        )))
    }
}
