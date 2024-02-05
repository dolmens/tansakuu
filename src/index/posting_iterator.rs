use std::io;

use crate::{DocId, END_POSITION};

pub trait PostingIterator {
    fn seek(&mut self, docid: DocId) -> io::Result<DocId>;

    fn seek_pos(&mut self, pos: u32) -> io::Result<u32> {
        let _ = pos;
        Ok(END_POSITION)
    }
}
