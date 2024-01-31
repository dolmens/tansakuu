use std::{collections::BTreeSet, fs::File, sync::Arc};

use crate::{
    index::IndexMerger,
    postings::{
        doc_list_encoder_builder,
        positions::{position_list_encoder_builder, PositionListEncode},
        DocListEncode, PostingFormat, PostingWriter, TermDictBuilder, TermInfo,
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

        let posting_format = PostingFormat::builder()
            .with_tflist()
            .with_position_list()
            .build();
        let doc_list_format = posting_format.doc_list_format().clone();

        let dict_path = directory.join(index.name().to_string() + ".dict");
        let dict_output_writer = File::create(dict_path).unwrap();
        let mut term_dict_writer = TermDictBuilder::new(dict_output_writer);

        let skip_list_path = directory.join(index.name().to_string() + ".skiplist");
        let skip_list_output_writer = File::create(skip_list_path).unwrap();
        let posting_path = directory.join(index.name().to_string() + ".posting");
        let posting_output_writer = File::create(posting_path).unwrap();

        let position_skip_list_path =
            directory.join(index.name().to_string() + ".positions.skiplist");
        let position_skip_list_output_writer = File::create(position_skip_list_path).unwrap();
        let position_list_path = directory.join(index.name().to_string() + ".positions");
        let position_list_output_writer = File::create(position_list_path).unwrap();

        let mut skip_list_start = 0;
        let mut doc_list_start = 0;
        let mut position_list_start = 0;
        let mut position_skip_list_start = 0;

        for term in &terms {
            let doc_list_encoder = doc_list_encoder_builder(doc_list_format.clone())
                .with_writer(&posting_output_writer)
                .with_skip_list_output_writer(&skip_list_output_writer)
                .build();

            let position_list_encoder = position_list_encoder_builder()
                .with_writer(&position_list_output_writer)
                .with_skip_list_output_writer(&position_skip_list_output_writer)
                .build();

            let mut posting_writer = PostingWriter::new(
                posting_format.clone(),
                doc_list_encoder,
                Some(position_list_encoder),
            );

            let tok = unsafe { std::str::from_utf8_unchecked(term) };

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
                    posting_writer.add_pos(0, 0).unwrap();
                    posting_writer.end_doc(docid).unwrap();
                }
            }

            posting_writer.flush().unwrap();

            let (doc_list_written_bytes, skip_list_written_bytes) =
                posting_writer.doc_list_encoder().written_bytes();

            let (position_list_written_bytes, position_skip_list_written_bytes) = posting_writer
                .position_list_encoder()
                .unwrap()
                .written_bytes();

            let skip_list_end = skip_list_start + skip_list_written_bytes;
            let doc_list_end = doc_list_start + doc_list_written_bytes;
            let position_list_end = position_list_start + position_list_written_bytes;
            let position_skip_list_end =
                position_skip_list_start + position_skip_list_written_bytes;

            let (doc_count, skip_list_item_count) = posting_writer.doc_list_encoder().item_count();
            let (position_list_item_count, position_skip_list_item_count) =
                posting_writer.position_list_encoder().unwrap().item_count();

            let term_info = TermInfo {
                skip_list_item_count,
                skip_list_start,
                skip_list_end,
                doc_count,
                doc_list_start,
                doc_list_end,
                position_skip_list_item_count,
                position_skip_list_start,
                position_skip_list_end,
                position_list_item_count,
                position_list_start,
                position_list_end,
            };

            skip_list_start = skip_list_end;
            doc_list_start = doc_list_end;
            position_skip_list_start = position_skip_list_end;
            position_list_start = position_list_end;

            term_dict_writer.insert(tok.as_bytes(), &term_info).unwrap();
        }

        term_dict_writer.finish().unwrap();
    }
}
