use std::{io::Cursor, sync::Arc};

use crate::{
    index::SegmentPosting,
    postings::{
        positions::PositionListReader, skip_list::SkipListReader, PostingFormat, PostingIterator,
        PostingReader,
    },
    DocId, END_DOCID,
};

use super::InvertedIndexPersistentSegmentData;

pub struct InvertedIndexPersistentSegmentReader {
    base_docid: DocId,
    index_data: Arc<InvertedIndexPersistentSegmentData>,
}

impl InvertedIndexPersistentSegmentReader {
    pub fn new(base_docid: DocId, index_data: Arc<InvertedIndexPersistentSegmentData>) -> Self {
        Self {
            base_docid,
            index_data,
        }
    }

    pub fn segment_posting(&self, tok: &str) -> crate::index::SegmentPosting {
        let mut docids = vec![];
        if let Some(term_info) = self.index_data.term_dict.get(tok).ok().unwrap() {
            let posting_format = PostingFormat::builder()
                .with_tflist()
                .with_position_list()
                .build();
            let skip_list_format = posting_format.skip_list_format().clone();
            let posting_data = self
                .index_data
                .posting_data
                .slice(term_info.posting_range());
            let posting_data = posting_data.as_slice();
            let posting_data = Cursor::new(posting_data);

            let skip_list_data = self
                .index_data
                .skip_list_data
                .slice(term_info.skip_list_range());
            let skip_list_data = skip_list_data.as_slice();
            let skip_list_reader = SkipListReader::open(
                skip_list_format,
                term_info.skip_list_item_count,
                skip_list_data,
            );

            let position_skip_list_data = self
                .index_data
                .position_skip_list_data
                .slice(term_info.position_skip_list_range());
            let position_skip_list_data = position_skip_list_data.as_slice();

            let position_list_data = self
                .index_data
                .position_list_data
                .slice(term_info.position_list_range());
            let position_list_data = position_list_data.as_slice();
            let position_list_data = Cursor::new(position_list_data);

            let position_list_reader = PositionListReader::open(
                term_info.position_list_item_count,
                position_list_data,
                term_info.position_skip_list_item_count,
                position_skip_list_data,
            );

            let mut posting_reader = PostingReader::open(
                posting_format.clone(),
                term_info.posting_item_count,
                posting_data,
                skip_list_reader,
                Some(position_list_reader),
            );

            let mut posting_iterator = PostingIterator::new(&mut posting_reader, &posting_format);
            let mut docid = 0;
            loop {
                docid = posting_iterator.seek(docid).unwrap();
                if docid == END_DOCID {
                    break;
                }
                docids.push(docid);
                docid = docid + 1;
            }
        }

        SegmentPosting::new(self.base_docid, docids)
    }
}
