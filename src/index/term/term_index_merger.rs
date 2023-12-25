use std::collections::HashMap;

use crate::{index::IndexMerger, DocId};

use super::{TermIndexSegmentData, TermIndexSerializerWriter};

#[derive(Default)]
pub struct TermIndexMerger {}

impl IndexMerger for TermIndexMerger {
    fn merge(
        &self,
        directory: &std::path::Path,
        index: &crate::schema::Index,
        segments: &[&dyn crate::index::IndexSegmentData],
        doc_counts: &[usize],
    ) {
        let path = directory.join(index.name());
        let mut writer = TermIndexSerializerWriter::new(path);
        let mut postings = HashMap::<String, Vec<DocId>>::new();
        let mut base_docid = 0;
        for (&segment, &doc_count) in segments.iter().zip(doc_counts.iter()) {
            let term_index_segment_data = segment.downcast_ref::<TermIndexSegmentData>().unwrap();
            for (term, segment_posting) in &term_index_segment_data.postings {
                let segment_posting = segment_posting
                    .iter()
                    .map(|&docid| docid + base_docid)
                    .collect();
                postings
                    .entry(term.to_string())
                    .and_modify(|p| p.extend(&segment_posting))
                    .or_insert(segment_posting);
            }
            base_docid += doc_count as DocId;
        }

        for (term, posting) in &postings {
            writer.start_term(term.to_string());
            for &docid in posting {
                writer.add_doc(&term, docid);
            }
            writer.end_term(&term);
        }
    }
}
