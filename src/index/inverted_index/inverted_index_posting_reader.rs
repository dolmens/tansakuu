use std::io;

use crate::{
    postings::{positions::PositionListBlock, DocListBlock},
    DocId,
};

use super::{posting_segment_reader::PostingSegmentReader, SegmentPosting, SegmentPostings};

pub struct InvertedIndexPostingReader<'a> {
    cursor: usize,
    segments: SegmentPostings<'a>,
    readers: Option<PostingSegmentReader<'a>>,
}

impl<'a> InvertedIndexPostingReader<'a> {
    pub fn new(segment_postings: Vec<SegmentPosting<'a>>) -> Self {
        Self {
            cursor: 0,
            segments: SegmentPostings::new(segment_postings),
            readers: None,
        }
    }

    pub fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        if self.readers.is_none() {
            if !self.move_to_segment(docid) {
                return Ok(false);
            }
        }
        loop {
            if self
                .readers
                .as_mut()
                .unwrap()
                .decode_doc_buffer(docid, doc_list_block)?
            {
                return Ok(true);
            }
            if !self.move_to_segment(docid) {
                return Ok(false);
            }
        }
    }

    pub fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        if let Some(segment_reader) = self.readers.as_mut() {
            segment_reader.decode_tf_buffer(doc_list_block)
        } else {
            Ok(false)
        }
    }

    pub fn decode_fieldmask_buffer(
        &mut self,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        if let Some(segment_reader) = self.readers.as_mut() {
            segment_reader.decode_fieldmask_buffer(doc_list_block)
        } else {
            Ok(false)
        }
    }

    pub fn decode_position_buffer(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if let Some(segment_reader) = self.readers.as_mut() {
            segment_reader.decode_position_buffer(from_ttf, position_list_block)
        } else {
            Ok(false)
        }
    }

    pub fn decode_next_position_record(
        &mut self,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if let Some(segment_reader) = self.readers.as_mut() {
            segment_reader.decode_next_position_record(position_list_block)
        } else {
            Ok(false)
        }
    }

    fn move_to_segment(&mut self, docid: DocId) -> bool {
        if let Some(cursor) = self.segments.locate_segment_from(docid, self.cursor) {
            self.readers = Some(PostingSegmentReader::open(unsafe {
                std::mem::transmute(self.segments.segment(cursor))
            }));
            self.cursor = cursor + 1;
            true
        } else {
            false
        }
    }
}
