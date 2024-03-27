use std::sync::Arc;

use crate::{
    index::inverted_index::{
        PersistentSegmentPosting, SegmentMultiPosting, SegmentMultiPostingData,
    },
    DocId,
};

use super::range_index_persistent_segment_data::RangeIndexPersistentSegmentData;

pub struct RangeIndexPersistentSegmentReader {
    base_docid: DocId,
    doc_count: usize,
    index_data: Arc<RangeIndexPersistentSegmentData>,
}

impl RangeIndexPersistentSegmentReader {
    pub fn new(
        base_docid: DocId,
        doc_count: usize,
        index_data: Arc<RangeIndexPersistentSegmentData>,
    ) -> Self {
        Self {
            base_docid,
            doc_count,
            index_data,
        }
    }

    pub fn lookup(
        &self,
        bottom_keys: &[u64],
        higher_keys: &[u64],
    ) -> Option<SegmentMultiPosting<'_>> {
        let postings: Vec<_> = bottom_keys
            .iter()
            .filter_map(|&k| {
                self.index_data
                    .bottom_posting_data
                    .term_dict
                    .get(k.to_be_bytes())
                    .ok()
                    .unwrap()
            })
            .map(|term_info| PersistentSegmentPosting {
                term_info,
                posting_data: &self.index_data.bottom_posting_data,
            })
            .chain(
                higher_keys
                    .iter()
                    .filter_map(|&k| {
                        self.index_data
                            .higher_posting_data
                            .term_dict
                            .get(k.to_be_bytes())
                            .ok()
                            .unwrap()
                    })
                    .map(|term_info| PersistentSegmentPosting {
                        term_info,
                        posting_data: &self.index_data.higher_posting_data,
                    }),
            )
            .collect();
        if !postings.is_empty() {
            Some(SegmentMultiPosting::new(
                self.base_docid,
                self.doc_count,
                SegmentMultiPostingData::Persistent(postings),
            ))
        } else {
            None
        }
    }
}
