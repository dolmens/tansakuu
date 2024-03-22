use std::sync::Arc;

use crate::{
    index::IndexReader,
    schema::Index,
    table::{SegmentMetaRegistry, TableData},
};

use super::{BitsetIndexBuildingSegmentData, BitsetPostingIterator, TernaryBitsetPostingIterator};

pub struct BitsetIndexReader {
    nullable: bool,
    segment_meta_registry: SegmentMetaRegistry,
    building_segments: Vec<Arc<BitsetIndexBuildingSegmentData>>,
}

impl BitsetIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let segment_meta_registry = table_data.segment_meta_registry().clone();

        let mut building_segments = vec![];
        for segment in table_data.building_segments() {
            let index_data = segment
                .data()
                .index_data()
                .index_data(index.name())
                .unwrap();
            let bitset_index_data = index_data
                .clone()
                .downcast_arc::<BitsetIndexBuildingSegmentData>()
                .ok()
                .unwrap();
            building_segments.push(bitset_index_data);
        }

        Self {
            nullable: index.is_nullable(),
            segment_meta_registry,
            building_segments,
        }
    }

    pub fn lookup<const POSITIVE: bool>(
        &self,
    ) -> Option<Box<dyn crate::index::PostingIterator + '_>> {
        let persistent_bitests = vec![];
        let building_bitsets: Vec<_> = self
            .building_segments
            .iter()
            .map(|segment| (&segment.values, segment.nulls.as_ref()))
            .collect();
        Some(Box::new(TernaryBitsetPostingIterator::<POSITIVE>::new(
            self.segment_meta_registry.clone(),
            &persistent_bitests,
            &building_bitsets,
        )))
    }

    pub fn null_posting_iterator(&self) -> Option<Box<dyn crate::index::PostingIterator + '_>> {
        let persistent_bitests = vec![];
        let building_bitsets: Vec<_> = self
            .building_segments
            .iter()
            .map(|segment| segment.nulls.as_ref().unwrap())
            .collect();
        Some(Box::new(BitsetPostingIterator::new(
            self.segment_meta_registry.clone(),
            &persistent_bitests,
            &building_bitsets,
        )))
    }
}

impl IndexReader for BitsetIndexReader {
    fn lookup<'a>(
        &'a self,
        term: &crate::query::Term,
    ) -> Option<Box<dyn crate::index::PostingIterator + 'a>> {
        if self.nullable && term.is_null() {
            return self.null_posting_iterator();
        }
        let positive = term.as_bool();
        if positive {
            self.lookup::<true>()
        } else {
            self.lookup::<false>()
        }
    }
}
