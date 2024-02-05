use std::io;

use crate::{
    index::PostingIterator,
    postings::{DocListBlock, PostingFormat},
    DocId, END_DOCID,
};

use super::{buffered_index_decoder::BufferedIndexDecoder, SegmentPosting};

pub struct BufferedPostingIterator<'a> {
    current_docid: DocId,
    block_cursor: usize,
    doc_list_block: DocListBlock,
    index_decoder: BufferedIndexDecoder<'a>,
}

impl<'a> BufferedPostingIterator<'a> {
    pub fn new(posting_format: &PostingFormat, segment_postings: Vec<SegmentPosting<'a>>) -> Self {
        let doc_list_block = DocListBlock::new(posting_format.doc_list_format());
        let index_decoder = BufferedIndexDecoder::new(segment_postings);

        Self {
            current_docid: 0,
            block_cursor: 0,
            doc_list_block,
            index_decoder,
        }
    }
}

impl<'a> PostingIterator for BufferedPostingIterator<'a> {
    fn seek(&mut self, docid: crate::DocId) -> io::Result<crate::DocId> {
        if self.block_cursor == self.doc_list_block.len || self.doc_list_block.last_docid < docid {
            if !self
                .index_decoder
                .decode_one_block(docid, &mut self.doc_list_block)?
            {
                return Ok(END_DOCID);
            }
            self.current_docid = self.doc_list_block.base_docid + self.doc_list_block.docids[0];
            self.block_cursor = 1;
        }

        while self.current_docid < docid {
            self.current_docid += self.doc_list_block.docids[self.block_cursor];
            self.block_cursor += 1;
        }

        Ok(self.current_docid)
    }
}
