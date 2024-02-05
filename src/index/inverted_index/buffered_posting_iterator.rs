use std::io;

use crate::{
    index::PostingIterator,
    postings::{positions::PositionListBlock, DocListBlock, PostingFormat},
    DocId, END_DOCID, END_POSITION, INVALID_DOCID,
};

use super::{buffered_index_decoder::BufferedIndexDecoder, SegmentPosting};

pub struct BufferedPostingIterator<'a> {
    current_docid: DocId,
    current_ttf: u64,
    current_tf: u32,
    current_fieldmask: u8,
    block_cursor: usize,
    doc_list_block: DocListBlock,
    position_docid: DocId,
    current_position: u32,
    current_position_index: u32,
    position_block_cursor: usize,
    position_list_block: Option<Box<PositionListBlock>>,
    index_decoder: BufferedIndexDecoder<'a>,
    posting_format: PostingFormat,
}

impl<'a> BufferedPostingIterator<'a> {
    pub fn new(posting_format: PostingFormat, segment_postings: Vec<SegmentPosting<'a>>) -> Self {
        let doc_list_block = DocListBlock::new(posting_format.doc_list_format());
        let index_decoder = BufferedIndexDecoder::new(segment_postings);

        Self {
            current_docid: 0,
            current_ttf: 0,
            current_tf: 0,
            current_fieldmask: 0,
            block_cursor: 0,
            doc_list_block,
            position_docid: INVALID_DOCID,
            current_position: 0,
            current_position_index: 0,
            position_block_cursor: 0,
            position_list_block: None,
            index_decoder,
            posting_format,
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
            if let Some(termfreqs) = &self.doc_list_block.termfreqs {
                self.current_ttf = self.doc_list_block.base_ttf;
                self.current_tf = termfreqs[0];
            }
            if let Some(fieldmasks) = &self.doc_list_block.fieldmasks {
                self.current_fieldmask = fieldmasks[0];
            }
            self.block_cursor = 1;
        }

        while self.current_docid < docid {
            self.current_docid += self.doc_list_block.docids[self.block_cursor];
            if let Some(termfreqs) = &self.doc_list_block.termfreqs {
                self.current_ttf += self.current_tf as u64;
                self.current_tf = termfreqs[self.block_cursor];
            }
            if let Some(fieldmasks) = &self.doc_list_block.fieldmasks {
                self.current_fieldmask = fieldmasks[self.block_cursor];
            }
            self.block_cursor += 1;
        }

        Ok(self.current_docid)
    }

    fn seek_pos(&mut self, pos: u32) -> io::Result<u32> {
        if !self.posting_format.has_tflist() || !self.posting_format.has_position_list() {
            return Ok(END_POSITION);
        }
        if self.position_list_block.is_none() {
            self.position_list_block = Some(Box::new(PositionListBlock::new()));
        }
        let position_list_block = self.position_list_block.as_mut().unwrap();

        if self.position_docid != self.current_docid {
            if self.position_block_cursor == position_list_block.len
                || self.current_ttf
                    > position_list_block.start_ttf + (position_list_block.len as u64)
            {
                if !self
                    .index_decoder
                    .decode_one_position_block(self.current_ttf, position_list_block)?
                {
                    return Ok(END_POSITION);
                }
            }
            self.position_docid = self.current_docid;
            self.position_block_cursor =
                (self.current_ttf - position_list_block.start_ttf) as usize;
            self.current_position = position_list_block.positions[self.position_block_cursor];
            self.current_position_index = 0;
            self.position_block_cursor += 1;
        }

        let pos = std::cmp::max(self.current_position, pos);
        while self.current_position < pos {
            self.current_position_index += 1;
            if self.current_position_index == self.current_tf {
                return Ok(END_POSITION);
            }
            if self.position_block_cursor == position_list_block.len {
                if !self
                    .index_decoder
                    .decode_one_position_block(self.current_ttf, position_list_block)?
                {
                    return Ok(END_POSITION);
                }
            }

            self.current_position += position_list_block.positions[self.position_block_cursor];
            self.position_block_cursor += 1;
        }

        Ok(self.current_position)
    }
}
