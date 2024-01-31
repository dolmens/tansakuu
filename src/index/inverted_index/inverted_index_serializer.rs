use std::{fs::File, sync::Arc};

use crate::{
    index::IndexSerializer,
    postings::{
        doc_list_encoder_builder,
        positions::{position_list_encoder_builder, PositionListEncode},
        DocListEncode, PostingFormat, PostingIterator, PostingWriter, TermDictBuilder, TermInfo,
    },
    schema::Index,
    END_DOCID, END_POSITION,
};

use super::InvertedIndexBuildingSegmentData;

pub struct InvertedIndexSerializer {
    index_name: String,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
}

impl InvertedIndexSerializer {
    pub fn new(index: &Index, index_data: Arc<InvertedIndexBuildingSegmentData>) -> Self {
        Self {
            index_name: index.name().to_string(),
            index_data,
        }
    }
}

impl IndexSerializer for InvertedIndexSerializer {
    fn serialize(&self, directory: &std::path::Path) {
        let posting_format = PostingFormat::builder()
            .with_tflist()
            .with_position_list()
            .build();
        let doc_list_format = posting_format.doc_list_format().clone();

        let dict_path = directory.join(self.index_name.clone() + ".dict");
        let dict_output_writer = File::create(dict_path).unwrap();
        let mut term_dict_writer = TermDictBuilder::new(dict_output_writer);

        let skip_list_path = directory.join(self.index_name.clone() + ".skiplist");
        let skip_list_output_writer = File::create(skip_list_path).unwrap();
        let posting_path = directory.join(self.index_name.clone() + ".posting");
        let posting_output_writer = File::create(posting_path).unwrap();

        let position_skip_list_path =
            directory.join(self.index_name.clone() + ".positions.skiplist");
        let position_skip_list_output_writer = File::create(position_skip_list_path).unwrap();
        let position_list_path = directory.join(self.index_name.clone() + ".positions");
        let position_list_output_writer = File::create(position_list_path).unwrap();

        let mut skip_list_start = 0;
        let mut posting_start = 0;
        let mut position_list_start = 0;
        let mut position_skip_list_start = 0;

        let mut postings: Vec<_> = self.index_data.postings.iter().collect();
        postings.sort_by(|a, b| a.0.cmp(b.0));

        for (tok, posting) in postings {
            let mut posting_iterator = PostingIterator::open(posting);

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

            let mut docid = 0;
            loop {
                docid = posting_iterator.seek(docid).unwrap();
                if docid == END_DOCID {
                    break;
                }
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
                        // get current tf...
                    }
                }
                posting_writer.end_doc(docid).unwrap();
                docid += 1;
            }

            posting_writer.flush().unwrap();

            let (posting_written_bytes, skip_list_written_bytes) =
                posting_writer.doc_list_encoder().written_bytes();

            let (position_list_written_bytes, position_skip_list_written_bytes) = posting_writer
                .position_list_encoder()
                .unwrap()
                .written_bytes();

            let skip_list_end = skip_list_start + skip_list_written_bytes;
            let posting_end = posting_start + posting_written_bytes;
            let position_list_end = position_list_start + position_list_written_bytes;
            let position_skip_list_end =
                position_skip_list_start + position_skip_list_written_bytes;

            let (posting_item_count, skip_list_item_count) =
                posting_writer.doc_list_encoder().item_count();
            let (position_list_item_count, position_skip_list_item_count) =
                posting_writer.position_list_encoder().unwrap().item_count();

            let term_info = TermInfo {
                skip_list_item_count,
                skip_list_start,
                skip_list_end,
                doc_count: posting_item_count,
                doc_list_start: posting_start,
                doc_list_end: posting_end,
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
