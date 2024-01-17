use std::{collections::BTreeSet, fs::File, sync::Arc};

use tantivy_common::CountingWriter;

use crate::{
    index::IndexMerger,
    postings::{
        PostingFormat, PostingWriter, SkipListWrite, SkipListWriter, TermDictBuilder, TermInfo,
    },
    DocId,
};

use super::{InvertedIndexPersistentSegmentData, InvertedIndexPersistentSegmentReader};

#[derive(Default)]
pub struct InvertedIndexMerger {}

impl IndexMerger for InvertedIndexMerger {
    fn merge(
        &self,
        directory: &std::path::Path,
        index: &crate::schema::Index,
        segments: &[&Arc<dyn crate::index::IndexSegmentData>],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        let mut terms = BTreeSet::<Vec<u8>>::new();
        for &segment in segments.iter() {
            let index_segment_data = segment
                .downcast_ref::<InvertedIndexPersistentSegmentData>()
                .unwrap();
            let term_dict = &index_segment_data.term_dict;
            for (term, _) in term_dict.iter() {
                terms.insert(term);
            }
        }

        let posting_format = PostingFormat::default();
        let skip_list_format = posting_format.skip_list_format().clone();

        let dict_path = directory.join(index.name().to_string() + ".dict");
        let term_dict_writer = File::create(dict_path).unwrap();
        let mut term_dict_writer = TermDictBuilder::new(term_dict_writer);

        let skip_list_path = directory.join(index.name().to_string() + ".skiplist");
        let skip_list_output_writer = File::create(skip_list_path).unwrap();
        let mut skip_list_counting_writer = CountingWriter::wrap(skip_list_output_writer);
        let posting_path = directory.join(index.name().to_string() + ".posting");
        let posting_output_writer = File::create(posting_path).unwrap();
        let mut posting_counting_writer = CountingWriter::wrap(posting_output_writer);

        let mut skip_start = 0;
        let mut posting_start = 0;

        for term in &terms {
            let tok = unsafe { std::str::from_utf8_unchecked(term) };

            let skip_list_writer =
                SkipListWriter::new(skip_list_format.clone(), skip_list_counting_writer);

            let mut posting_writer = PostingWriter::new_with_skip_list(
                posting_format.clone(),
                posting_counting_writer,
                skip_list_writer,
            );

            for (&segment, segment_docid_mappings) in segments.iter().zip(docid_mappings.iter()) {
                let index_segment_data = segment
                    .clone()
                    .downcast_arc::<InvertedIndexPersistentSegmentData>()
                    .ok()
                    .unwrap();
                let segment_reader =
                    InvertedIndexPersistentSegmentReader::new(0, index_segment_data);
                let posting = segment_reader.segment_posting(tok);
                let docids: Vec<_> = posting
                    .docids
                    .iter()
                    .flat_map(|&docid| segment_docid_mappings[docid as usize])
                    .collect();
                for docid in docids {
                    posting_writer.add_pos(1);
                    posting_writer.end_doc(docid);
                }
            }

            posting_writer.flush().unwrap();
            let posting_item_count = posting_writer.flush_info().flushed_count();

            let (posting_writer, mut skip_list_writer) = posting_writer.finish();

            skip_list_writer.flush().unwrap();
            let skip_item_count = skip_list_writer.flush_info().flushed_count();

            posting_counting_writer = posting_writer;
            skip_list_counting_writer = skip_list_writer.finish();

            let skip_end = skip_list_counting_writer.written_bytes() as usize;
            let posting_end = posting_counting_writer.written_bytes() as usize;
            let term_info = TermInfo {
                skip_item_count,
                skip_start,
                skip_end,
                posting_item_count,
                posting_start,
                posting_end,
            };
            skip_start = skip_end;
            posting_start = posting_end;
            term_dict_writer.insert(tok.as_bytes(), &term_info).unwrap();
        }

        term_dict_writer.finish().unwrap();
    }
}
