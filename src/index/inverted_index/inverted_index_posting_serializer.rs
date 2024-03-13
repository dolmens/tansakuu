use std::{io::Write, path::Path};

use tantivy_common::TerminatingWrite;

use crate::{
    postings::{
        doc_list_encoder_builder,
        positions::{position_list_encoder_builder, PositionListEncode},
        DocListEncode, PostingFormat, PostingIterator, PostingWriter, TermDictBuilder, TermInfo,
    },
    Directory, DocId, END_DOCID, END_POSITION,
};

use super::BuildingPostingData;

#[derive(Default)]
pub struct InvertedIndexPostingSerializer {}

impl InvertedIndexPostingSerializer {
    pub fn serialize(
        &self,
        name: &str,
        posting_format: &PostingFormat,
        posting_data: &BuildingPostingData,
        directory: &dyn Directory,
        index_path: &Path,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    ) {
        let dict_path = index_path.join(name.to_string() + ".dict");
        let dict_output_writer = directory.open_write(&dict_path).unwrap();
        let mut term_dict_writer = TermDictBuilder::new(dict_output_writer);

        let skip_list_path = index_path.join(name.to_string() + ".skiplist");
        let mut skip_list_output_writer = directory.open_write(&skip_list_path).unwrap();
        let posting_path = index_path.join(name.to_string() + ".posting");
        let mut posting_output_writer = directory.open_write(&posting_path).unwrap();

        let mut position_skip_list_output_writer = if posting_format.has_position_list() {
            let position_skip_list_path = index_path.join(name.to_string() + ".positions.skiplist");
            Some(directory.open_write(&position_skip_list_path).unwrap())
        } else {
            None
        };

        let mut position_list_output_writer = if posting_format.has_position_list() {
            let position_list_path = index_path.join(name.to_string() + ".positions");
            Some(directory.open_write(&position_list_path).unwrap())
        } else {
            None
        };

        let mut skip_list_start = 0;
        let mut doc_list_start = 0;
        let mut position_list_start = 0;
        let mut position_skip_list_start = 0;

        let mut postings: Vec<_> = posting_data.iter().map(|(k, v)| (k.clone(), v)).collect();
        postings.sort_by(|a, b| a.0.to_be_bytes().cmp(&b.0.to_be_bytes()));

        for (hashkey, posting) in postings {
            let mut posting_iterator = PostingIterator::open_building_posting_list(posting);

            let doc_list_encoder =
                doc_list_encoder_builder(posting_format.doc_list_format().clone())
                    .with_writer(posting_output_writer.by_ref())
                    .with_skip_list_output_writer(skip_list_output_writer.by_ref())
                    .build();

            let position_list_encoder = if posting_format.has_position_list() {
                Some(
                    position_list_encoder_builder()
                        .with_writer(
                            position_list_output_writer
                                .as_mut()
                                .map(|w| w.by_ref())
                                .unwrap(),
                        )
                        .with_skip_list_output_writer(
                            position_skip_list_output_writer
                                .as_mut()
                                .map(|w| w.by_ref())
                                .unwrap(),
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

            let mut docid = 0;
            loop {
                docid = posting_iterator.seek(docid).unwrap();
                if docid == END_DOCID {
                    break;
                }
                if let Some(docid) = if let Some(docid_mapping) = docid_mapping {
                    docid_mapping[docid as usize]
                } else {
                    Some(docid)
                } {
                    if posting_format.has_tflist() {
                        if posting_format.has_position_list() {
                            let mut pos = 0;
                            loop {
                                pos = posting_iterator.seek_pos(pos).unwrap();
                                if pos == END_POSITION {
                                    posting_writer.add_pos(0, pos).unwrap();
                                    break;
                                }
                                pos += 1;
                            }
                        } else {
                            let tf = posting_iterator.get_current_tf().unwrap();
                            for _ in 0..tf {
                                posting_writer.add_pos(0, 0).unwrap();
                            }
                        }
                    }
                    if posting_format.has_fieldmask() {
                        let fieldmask = posting_iterator.get_current_fieldmask().unwrap();
                        posting_writer.set_fieldmask(fieldmask);
                    }
                    posting_writer.end_doc(docid).unwrap();
                }
                docid += 1;
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

            let df = posting_writer.doc_list_encoder().df();
            let ttf = posting_writer
                .position_list_encoder()
                .map_or(0, |encoder| encoder.ttf());

            let term_info = TermInfo {
                df,
                doc_list_start,
                doc_list_end,
                skip_list_start,
                skip_list_end,

                ttf,
                position_list_start,
                position_list_end,
                position_skip_list_start,
                position_skip_list_end,
            };

            skip_list_start = skip_list_end;
            doc_list_start = doc_list_end;
            position_skip_list_start = position_skip_list_end;
            position_list_start = position_list_end;

            term_dict_writer
                .insert(hashkey.to_be_bytes(), &term_info)
                .unwrap();
        }

        skip_list_output_writer.terminate().unwrap();
        posting_output_writer.terminate().unwrap();
        position_skip_list_output_writer.map(|w| w.terminate().unwrap());
        position_list_output_writer.map(|w| w.terminate().unwrap());

        term_dict_writer.finish().unwrap().terminate().unwrap();
    }
}
