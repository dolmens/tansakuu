use crate::index::PostingIterator;

use super::{PostingSegmentMultiReader, SegmentMultiPosting};

pub struct MultiPostingIterator<'a> {
    segment_reader: Option<PostingSegmentMultiReader<'a>>,
    cursor: usize,
    postings: Vec<SegmentMultiPosting<'a>>,
}

impl<'a> PostingIterator for MultiPostingIterator<'a> {
    fn seek(&mut self, docid: crate::DocId) -> std::io::Result<crate::DocId> {
        unimplemented!()
    }

    fn seek_pos(&mut self, _pos: u32) -> std::io::Result<u32> {
        unimplemented!()
    }
}
