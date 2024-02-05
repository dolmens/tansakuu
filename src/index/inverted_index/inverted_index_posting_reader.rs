use std::io;

use crate::{
    postings::{positions::PositionListBlock, DocListBlock},
    DocId, INVALID_DOCID,
};

use super::{
    inverted_index_posting_segment_reader::InvertedIndexPostingSegmentReader, SegmentPosting,
};

pub struct InvertedIndexPostingReader<'a> {
    segment_reader: Option<InvertedIndexPostingSegmentReader<'a>>,
    cursor: usize,
    postings: Vec<SegmentPosting<'a>>,
}

impl<'a> InvertedIndexPostingReader<'a> {
    pub fn new(segment_postings: Vec<SegmentPosting<'a>>) -> Self {
        Self {
            segment_reader: None,
            cursor: 0,
            postings: segment_postings,
        }
    }

    pub fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        if self.segment_reader.is_none() {
            self.move_to_segment(docid);
        }
        loop {
            if self
                .segment_reader
                .as_mut()
                .unwrap()
                .decode_one_block(docid, doc_list_block)?
            {
                return Ok(true);
            }
            if !self.move_to_segment(docid) {
                return Ok(false);
            }
        }
    }

    pub fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if let Some(segment_reader) = self.segment_reader.as_mut() {
            segment_reader.decode_one_position_block(from_ttf, position_list_block)
        } else {
            Ok(false)
        }
    }

    fn move_to_segment(&mut self, docid: DocId) -> bool {
        let cursor = self.locate_segment(self.cursor, docid);
        if cursor >= self.postings.len() {
            return false;
        }
        self.segment_reader = Some(InvertedIndexPostingSegmentReader::open(unsafe {
            std::mem::transmute(&self.postings[cursor])
        }));
        self.cursor = cursor + 1;
        true
    }

    fn locate_segment(&self, cursor: usize, docid: DocId) -> usize {
        let curr_seg_base_docid = self.segment_base_docid(cursor);
        if curr_seg_base_docid == INVALID_DOCID {
            return cursor;
        }
        let mut cursor = cursor;
        let mut next_seg_base_docid = self.segment_base_docid(cursor + 1);
        while next_seg_base_docid != INVALID_DOCID && docid >= next_seg_base_docid {
            cursor += 1;
            next_seg_base_docid = self.segment_base_docid(cursor + 1);
        }
        cursor
    }

    fn segment_base_docid(&self, cursor: usize) -> DocId {
        if cursor >= self.postings.len() {
            INVALID_DOCID
        } else {
            self.postings[cursor].base_docid()
        }
    }
}
