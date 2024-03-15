use std::{collections::BTreeSet, path::Path};

use crate::{postings::PostingIterator, Directory, DocId, END_DOCID, END_POSITION};

use super::{
    persistent_posting_reader::PersistentPostingReader, InvertedIndexPostingSerializeWriter,
    PersistentPostingData,
};

#[derive(Default)]
pub struct InvertedIndexPostingMerger {}

impl InvertedIndexPostingMerger {
    pub fn merge(
        &self,
        directory: &dyn Directory,
        path: &Path,
        name: &str,
        posting_datas: &[&PersistentPostingData],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        let mut terms = BTreeSet::<Vec<u8>>::new();
        for &posting_data in posting_datas {
            for (term, _) in posting_data.term_dict.iter() {
                terms.insert(term);
            }
        }

        let posting_format = posting_datas[0].posting_format;

        let mut serialize_writer =
            InvertedIndexPostingSerializeWriter::new(directory, path, name, posting_format);

        for term in &terms {
            // TODO: If we can find out that this term's documents were all deleted,
            // then we just skip it.
            let hashkey = term
                .as_slice()
                .try_into()
                .map_or(0, |b| u64::from_be_bytes(b));
            if hashkey == 0 {
                continue;
            }

            let mut posting_writer = serialize_writer.start_token(hashkey);

            for (&posting_data, docid_mapping) in posting_datas.iter().zip(docid_mappings.iter()) {
                if let Some(posting_reader) =
                    PersistentPostingReader::lookup(posting_data, hashkey).unwrap()
                {
                    let mut posting_iterator =
                        PostingIterator::new(posting_format.clone(), posting_reader);
                    let mut docid = 0;
                    loop {
                        docid = posting_iterator.seek(docid).unwrap();
                        if docid == END_DOCID {
                            break;
                        }
                        if let Some(new_docid) = docid_mapping[docid as usize] {
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
                            posting_writer.end_doc(new_docid).unwrap();
                        }
                        docid += 1;
                    }
                }
            }

            let status = posting_writer.finish().unwrap();
            serialize_writer.end_token(status);
        }

        serialize_writer.finish();
    }
}
