use std::{io::Cursor, sync::Arc};

use crate::{
    index::SegmentPosting,
    postings::{positions::PositionListDecoder, DocListDecoder, PostingFormat, PostingIterator},
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
            let doc_list_format = posting_format.doc_list_format().clone();
            let doc_list_data = self
                .index_data
                .posting_data
                .slice(term_info.doc_list_range());
            let doc_list_data = doc_list_data.as_slice();
            let doc_list_data = Cursor::new(doc_list_data);

            let skip_list_data = self
                .index_data
                .skip_list_data
                .slice(term_info.skip_list_range());
            let skip_list_data = skip_list_data.as_slice();
            let skip_list_data = Cursor::new(skip_list_data);

            let doc_list_decoder = DocListDecoder::open(
                doc_list_format,
                term_info.doc_count,
                doc_list_data,
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

            let position_list_decoder = Some(PositionListDecoder::open(
                term_info.position_list_item_count,
                position_list_data,
                term_info.position_skip_list_item_count,
                position_skip_list_data,
            ));

            let mut posting_iterator =
                PostingIterator::new(posting_format, doc_list_decoder, position_list_decoder);

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
