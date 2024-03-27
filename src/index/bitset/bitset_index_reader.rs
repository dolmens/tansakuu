use std::sync::Arc;

use crate::{
    index::{
        bitset::BitsetSegmentPosting, AllDocsPostingIterator, AndNotPostingIterator, IndexReader,
        NegatedPostingIterator,
    },
    schema::Index,
    table::{segment::BuildingDocCount, SegmentRegistry, TableData},
    DocId,
};

use super::{
    BitsetIndexBuildingSegmentData, BitsetIndexPersistentSegmentData, BitsetPostingIterator,
};

pub struct BitsetIndexReader {
    nullable: bool,
    persistent_segments: Vec<BitsetIndexPersistentSegmentReader>,
    building_segments: Vec<BitsetIndexBuildingSegmentReader>,
}

struct BitsetIndexPersistentSegmentReader {
    base_docid: DocId,
    doc_count: usize,
    index_data: Arc<BitsetIndexPersistentSegmentData>,
}

struct BitsetIndexBuildingSegmentReader {
    base_docid: DocId,
    doc_count: usize,
    building_doc_count: BuildingDocCount,
    index_data: Arc<BitsetIndexBuildingSegmentData>,
}

impl BitsetIndexReader {
    pub fn new(index: &Index, table_data: &TableData) -> Self {
        let mut persistent_segments = vec![];
        for segment in table_data.persistent_segments() {
            let index_data = segment.data().index_data(index.name()).unwrap();
            let bitset_index_data = index_data.clone().downcast_arc().ok().unwrap();
            persistent_segments.push(BitsetIndexPersistentSegmentReader {
                base_docid: segment.meta().base_docid(),
                doc_count: segment.meta().doc_count(),
                index_data: bitset_index_data,
            });
        }

        let mut building_segments = vec![];
        for segment in table_data.building_segments() {
            let base_docid = segment.meta().base_docid();
            let doc_count = segment.meta().doc_count();
            let building_doc_count = segment.data().doc_count().clone();
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
            building_segments.push(BitsetIndexBuildingSegmentReader {
                base_docid,
                doc_count,
                building_doc_count,
                index_data: bitset_index_data,
            });
        }

        Self {
            nullable: index.is_nullable(),
            persistent_segments,
            building_segments,
        }
    }

    pub fn lookup_positive(&self) -> Option<Box<dyn crate::index::PostingIterator + '_>> {
        self.lookup_positive_inner()
            .map(|posting_iterator| Box::new(posting_iterator) as _)
    }

    pub fn lookup_positive_inner(&self) -> Option<BitsetPostingIterator> {
        let mut segment_postings = vec![];

        for segment in &self.persistent_segments {
            if let Some(values) = segment.index_data.values.as_ref() {
                let base_docid = segment.base_docid;
                let doc_count = segment.doc_count;
                segment_postings.push(BitsetSegmentPosting::new_immutable(
                    base_docid, doc_count, values,
                ));
            }
        }

        for segment in &self.building_segments {
            if !segment.index_data.values.is_empty() {
                let base_docid = segment.base_docid;
                let doc_count = if segment.doc_count > 0 {
                    segment.doc_count
                } else {
                    segment.building_doc_count.get()
                };
                segment_postings.push(BitsetSegmentPosting::new_mutable(
                    base_docid,
                    doc_count,
                    &segment.index_data.values,
                ));
            }
        }

        if !segment_postings.is_empty() {
            Some(BitsetPostingIterator::new(segment_postings))
        } else {
            None
        }
    }

    pub fn lookup_negative(&self) -> Option<Box<dyn crate::index::PostingIterator + '_>> {
        let mut segment_registry = SegmentRegistry::default();

        for segment in &self.persistent_segments {
            let base_docid = segment.base_docid;
            let doc_count = segment.doc_count;
            segment_registry.add_static_segment(base_docid, doc_count);
        }

        for segment in &self.building_segments {
            let base_docid = segment.base_docid;
            let doc_count = if segment.doc_count > 0 {
                segment.doc_count
            } else {
                segment.building_doc_count.get()
            };
            segment_registry.add_static_segment(base_docid, doc_count);
        }

        if !segment_registry.is_empty() {
            if let Some(positive_posting_iteartor) = self.lookup_positive_inner() {
                let negative =
                    NegatedPostingIterator::new(segment_registry, positive_posting_iteartor);
                if self.nullable {
                    if let Some(nulls) = self.lookup_null_inner() {
                        Some(Box::new(AndNotPostingIterator::new(negative, nulls)))
                    } else {
                        Some(Box::new(negative))
                    }
                } else {
                    Some(Box::new(negative))
                }
            } else {
                Some(Box::new(AllDocsPostingIterator::new(segment_registry)))
            }
        } else {
            None
        }
    }

    pub fn lookup_null(&self) -> Option<Box<dyn crate::index::PostingIterator + '_>> {
        if self.nullable {
            self.lookup_null_inner()
                .map(|posting_iterator| Box::new(posting_iterator) as _)
        } else {
            None
        }
    }

    pub fn lookup_null_inner(&self) -> Option<BitsetPostingIterator> {
        let mut segment_postings = vec![];

        for segment in &self.persistent_segments {
            if let Some(nulls) = segment.index_data.nulls.as_ref() {
                let base_docid = segment.base_docid;
                let doc_count = segment.doc_count;
                segment_postings.push(BitsetSegmentPosting::new_immutable(
                    base_docid, doc_count, nulls,
                ));
            }
        }

        for segment in &self.building_segments {
            if let Some(nulls) = segment.index_data.nulls.as_ref() {
                if !nulls.is_empty() {
                    let base_docid = segment.base_docid;
                    let doc_count = if segment.doc_count > 0 {
                        segment.doc_count
                    } else {
                        segment.building_doc_count.get()
                    };
                    segment_postings.push(BitsetSegmentPosting::new_mutable(
                        base_docid, doc_count, nulls,
                    ));
                }
            }
        }

        if !segment_postings.is_empty() {
            Some(BitsetPostingIterator::new(segment_postings))
        } else {
            None
        }
    }

    pub fn lookup_non_null(&self) -> Option<Box<dyn crate::index::PostingIterator + '_>> {
        let mut segment_registry = SegmentRegistry::default();

        for segment in &self.persistent_segments {
            let base_docid = segment.base_docid;
            let doc_count = segment.doc_count;
            segment_registry.add_static_segment(base_docid, doc_count);
        }

        for segment in &self.building_segments {
            let base_docid = segment.base_docid;
            let doc_count = if segment.doc_count > 0 {
                segment.doc_count
            } else {
                segment.building_doc_count.get()
            };
            segment_registry.add_static_segment(base_docid, doc_count);
        }

        if !segment_registry.is_empty() {
            if self.nullable {
                if let Some(nulls) = self.lookup_null_inner() {
                    Some(Box::new(NegatedPostingIterator::new(
                        segment_registry,
                        nulls,
                    )))
                } else {
                    Some(Box::new(AllDocsPostingIterator::new(segment_registry)))
                }
            } else {
                Some(Box::new(AllDocsPostingIterator::new(segment_registry)))
            }
        } else {
            None
        }
    }
}

impl IndexReader for BitsetIndexReader {
    fn lookup<'a>(
        &'a self,
        term: &crate::query::Term,
    ) -> Option<Box<dyn crate::index::PostingIterator + 'a>> {
        if term.is_null() {
            return self.lookup_null();
        } else if term.is_non_null() {
            return self.lookup_non_null();
        }

        let positive = term.as_bool();
        if positive {
            self.lookup_positive()
        } else {
            self.lookup_negative()
        }
    }
}
