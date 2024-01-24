use std::{collections::BTreeSet, fs::File, sync::Arc};

use crate::{
    index::IndexMerger,
    postings::{
        positions::PositionListWriter,
        skip_list::{SkipListFormat, SkipListWriter},
        PostingFormat, PostingWriter, TermDictBuilder, TermInfo,
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
        let skip_list_format = posting_format.skip_list_format().clone();

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
        let mut posting_start = 0;
        let mut position_list_start = 0;
        let mut position_skip_list_start = 0;

        for term in &terms {
            let skip_list_writer =
                SkipListWriter::new(skip_list_format.clone(), &skip_list_output_writer);

            let position_skip_list_writer =
                SkipListWriter::new(SkipListFormat::default(), &position_skip_list_output_writer);
            let position_list_writer = Some(PositionListWriter::new(
                &position_list_output_writer,
                position_skip_list_writer,
            ));

            let mut posting_writer = PostingWriter::new(
                posting_format.clone(),
                &posting_output_writer,
                skip_list_writer,
                position_list_writer,
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

            let (posting_written_bytes, skip_list_written_bytes) = posting_writer.written_bytes();

            let (position_list_written_bytes, position_skip_list_written_bytes) =
                posting_writer.position_list_written_bytes().unwrap();

            let skip_list_end = skip_list_start + skip_list_written_bytes;
            let posting_end = posting_start + posting_written_bytes;
            let position_list_end = position_list_start + position_list_written_bytes;
            let position_skip_list_end =
                position_skip_list_start + position_skip_list_written_bytes;

            let (posting_item_count, skip_list_item_count) = posting_writer.item_count();
            let (position_list_item_count, position_skip_list_item_count) =
                posting_writer.position_list_item_count().unwrap();

            let term_info = TermInfo {
                skip_list_item_count,
                skip_list_start,
                skip_list_end,
                posting_item_count,
                posting_start,
                posting_end,
                position_skip_list_item_count,
                position_skip_list_start,
                position_skip_list_end,
                position_list_item_count,
                position_list_start,
                position_list_end,
            };

            skip_list_start = skip_list_end;
            posting_start = posting_end;
            position_skip_list_start = position_skip_list_end;
            position_list_start = position_list_end;

            term_dict_writer.insert(tok.as_bytes(), &term_info).unwrap();
        }

        term_dict_writer.finish().unwrap();
    }
}
