use std::{collections::BTreeSet, fs::File, sync::Arc};

use crate::{
    index::IndexMerger,
    postings::{
        doc_list_encoder_builder,
        positions::{position_list_encoder_builder, PositionListEncode},
        DocListEncode, PostingFormat, PostingIterator, PostingWriter, TermDictBuilder, TermInfo,
    },
    schema::IndexType,
    DocId, END_DOCID,
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
        let posting_format = if let IndexType::Text(text_index_options) = index.index_type() {
            PostingFormat::builder()
                .with_text_index_options(text_index_options)
                .build()
        } else {
            PostingFormat::builder().build()
        };
        let doc_list_format = posting_format.doc_list_format().clone();

        let dict_path = directory.join(index.name().to_string() + ".dict");
        let dict_output_writer = File::create(dict_path).unwrap();
        let mut term_dict_writer = TermDictBuilder::new(dict_output_writer);

        let skip_list_path = directory.join(index.name().to_string() + ".skiplist");
        let skip_list_output_writer = File::create(skip_list_path).unwrap();
        let posting_path = directory.join(index.name().to_string() + ".posting");
        let posting_output_writer = File::create(posting_path).unwrap();

        let position_skip_list_output_writer = if posting_format.has_position_list() {
            let position_skip_list_path =
                directory.join(index.name().to_string() + ".positions.skiplist");
            Some(File::create(position_skip_list_path).unwrap())
        } else {
            None
        };
        let position_list_output_writer = if posting_format.has_position_list() {
            let position_list_path = directory.join(index.name().to_string() + ".positions");
            Some(File::create(position_list_path).unwrap())
        } else {
            None
        };

        let mut skip_list_start = 0;
        let mut doc_list_start = 0;
        let mut position_list_start = 0;
        let mut position_skip_list_start = 0;

        for term in &terms {
            let hashkey = term
                .as_slice()
                .try_into()
                .map_or(0, |b| u64::from_be_bytes(b));
            if hashkey == 0 {
                continue;
            }
            let doc_list_encoder = doc_list_encoder_builder(doc_list_format.clone())
                .with_writer(&posting_output_writer)
                .with_skip_list_output_writer(&skip_list_output_writer)
                .build();

            let position_list_encoder = if posting_format.has_position_list() {
                Some(
                    position_list_encoder_builder()
                        .with_writer(position_list_output_writer.as_ref().unwrap())
                        .with_skip_list_output_writer(
                            position_skip_list_output_writer.as_ref().unwrap(),
                        )
                        .build(),
                )
            } else {
                None
            };

            let mut posting_writer = PostingWriter::new(
                posting_format.clone(),
                doc_list_encoder,
                position_list_encoder,
            );

            for (&segment, segment_docid_mappings) in segments.iter().zip(docid_mappings.iter()) {
                let index_segment_data = segment
                    .clone()
                    .downcast_arc::<InvertedIndexPersistentSegmentData>()
                    .ok()
                    .unwrap();
                let segment_reader =
                    InvertedIndexPersistentSegmentReader::new(0, index_segment_data);
                if let Some(posting_reader) = segment_reader.posting_reader(hashkey) {
                    let mut posting_iterator =
                        PostingIterator::new(posting_format.clone(), posting_reader);
                    let mut docid = 0;
                    loop {
                        docid = posting_iterator.seek(docid).unwrap();
                        if docid == END_DOCID {
                            break;
                        }
                        if let Some(new_docid) = segment_docid_mappings[docid as usize] {
                            posting_writer.add_pos(0, 0).unwrap();
                            posting_writer.end_doc(new_docid).unwrap();
                        }
                        docid += 1;
                    }
                }
            }

            posting_writer.flush().unwrap();

            let (doc_list_written_bytes, skip_list_written_bytes) =
                posting_writer.doc_list_encoder().written_bytes();

            let (position_list_written_bytes, position_skip_list_written_bytes) = posting_writer
                .position_list_encoder()
                .map_or((0, 0), |encoder| encoder.written_bytes());

            let skip_list_end = skip_list_start + skip_list_written_bytes;
            let doc_list_end = doc_list_start + doc_list_written_bytes;
            let position_list_end = position_list_start + position_list_written_bytes;
            let position_skip_list_end =
                position_skip_list_start + position_skip_list_written_bytes;

            let (doc_count, skip_list_item_count) = posting_writer.doc_list_encoder().item_count();
            let (position_list_item_count, position_skip_list_item_count) = posting_writer
                .position_list_encoder()
                .map_or((0, 0), |encoder| encoder.item_count());

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

            term_dict_writer.insert(term, &term_info).unwrap();
        }

        term_dict_writer.finish().unwrap();
    }
}
