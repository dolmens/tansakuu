use std::path::Path;

use crate::{
    postings::{PostingFormat, PostingIterator},
    Directory, DocId, END_DOCID, END_POSITION,
};

use super::{BuildingPostingData, InvertedIndexPostingSerializeWriter};

#[derive(Default)]
pub struct InvertedIndexPostingSerializer {}

impl InvertedIndexPostingSerializer {
    pub fn serialize(
        &self,
        name: &str,
        posting_format: PostingFormat,
        posting_data: &BuildingPostingData,
        directory: &dyn Directory,
        index_path: &Path,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    ) {
        let mut serialize_writer =
            InvertedIndexPostingSerializeWriter::new(directory, index_path, name, posting_format);

        let mut postings: Vec<_> = posting_data.iter().map(|(k, v)| (k.clone(), v)).collect();
        postings.sort_by(|a, b| a.0.to_be_bytes().cmp(&b.0.to_be_bytes()));

        for (hashkey, posting) in postings {
            let mut posting_iterator = PostingIterator::open_building_posting_list(posting);

            let mut posting_writer = serialize_writer.start_token(hashkey);

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
                                    break;
                                }
                                posting_writer.add_pos(0, pos).unwrap();
                                // TODO: don't need inc pos
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

            let status = posting_writer.finish().unwrap();
            serialize_writer.end_token(status);
        }

        serialize_writer.finish();
    }
}
