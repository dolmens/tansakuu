use std::collections::HashMap;

use crate::{index::IndexMerger, DocId};

use super::{InvertedIndexPersistentSegmentData};

#[derive(Default)]
pub struct InvertedIndexMerger {}

impl IndexMerger for InvertedIndexMerger {
    fn merge(
        &self,
        directory: &std::path::Path,
        index: &crate::schema::Index,
        segments: &[&dyn crate::index::IndexSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        unimplemented!()
        // let path = directory.join(index.name());
        // let mut writer = InvertedIndexSerializerWriter::new(path);
        // let mut postings = HashMap::<String, Vec<DocId>>::new();
        // for (&segment, segment_docid_mappings) in segments.iter().zip(docid_mappings.iter()) {
        //     let index_segment_data = segment
        //         .downcast_ref::<InvertedIndexPersistentSegmentData>()
        //         .unwrap();
        //     for (term, segment_posting) in &index_segment_data.postings {
        //         let segment_posting = segment_posting
        //             .iter()
        //             .filter_map(|&docid| segment_docid_mappings[docid as usize])
        //             .collect();
        //         postings
        //             .entry(term.to_string())
        //             .and_modify(|p| p.extend(&segment_posting))
        //             .or_insert(segment_posting);
        //     }
        // }

        // for (term, posting) in &postings {
        //     writer.start_term(term.to_string());
        //     for &docid in posting {
        //         writer.add_doc(&term, docid);
        //     }
        //     writer.end_term(&term);
        // }
    }
}
