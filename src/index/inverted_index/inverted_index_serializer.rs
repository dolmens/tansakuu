use std::{fs::File, sync::Arc};

use tantivy_common::CountingWriter;

use crate::{
    index::IndexSerializer,
    postings::{
        BuildingPostingReader, PostingFormat, PostingIterator, PostingWriter, SkipListWrite,
        SkipListWriter, TermDictBuilder, TermInfo,
    },
    schema::Index,
    END_DOCID, INVALID_DOCID,
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
        let posting_format = PostingFormat::default();
        let skip_list_format = posting_format.skip_list_format().clone();

        let dict_path = directory.join(self.index_name.clone() + ".dict");
        let term_dict_writer = File::create(dict_path).unwrap();
        let mut term_dict_writer = TermDictBuilder::new(term_dict_writer);

        let skip_list_path = directory.join(self.index_name.clone() + ".skiplist");
        let skip_list_output_writer = File::create(skip_list_path).unwrap();
        let mut skip_list_counting_writer = CountingWriter::wrap(skip_list_output_writer);
        let posting_path = directory.join(self.index_name.clone() + ".posting");
        let posting_output_writer = File::create(posting_path).unwrap();
        let mut posting_counting_writer = CountingWriter::wrap(posting_output_writer);

        let mut skip_start = 0;
        let mut posting_start = 0;

        let mut postings: Vec<_> = self.index_data.postings.iter().collect();
        postings.sort_by(|a, b| a.0.cmp(b.0));

        for (tok, posting) in postings {
            let mut posting_reader = BuildingPostingReader::open(posting);
            let mut posting_iterator = PostingIterator::new(&mut posting_reader);

            let skip_list_writer =
                SkipListWriter::new(skip_list_format.clone(), skip_list_counting_writer);

            let mut posting_writer = PostingWriter::new_with_skip_list(
                posting_format.clone(),
                posting_counting_writer,
                skip_list_writer,
            );

            let mut docid = INVALID_DOCID;
            loop {
                docid = posting_iterator.seek(docid.wrapping_add(1)).unwrap();
                if docid == END_DOCID {
                    break;
                }
                posting_writer.add_pos(1);
                posting_writer.end_doc(docid);
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
