use std::{io::Cursor, sync::Arc};

use crate::{
    index::SegmentPosting,
    postings::{PostingFormat, PostingIterator, PostingReader, SkipListFormat, SkipListReader},
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
            let skip_list_reader = SkipListReader::open(
                SkipListFormat::default(),
                term_info.skip_item_count,
                self.index_data.skip_data.slice(term_info.skip_range()),
            );
            let posting_data = self
                .index_data
                .posting_data
                .slice(term_info.posting_range());
            let posting_data = posting_data.as_slice();
            let posting_data = Cursor::new(posting_data);
            let mut posting_reader = PostingReader::open_with_skip_list(
                PostingFormat::default(),
                term_info.posting_item_count,
                posting_data,
                skip_list_reader,
            );
            let mut posting_iterator = PostingIterator::new(&mut posting_reader);
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
