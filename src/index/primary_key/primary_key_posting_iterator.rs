use std::io;

use crate::{index::PostingIterator, DocId, END_DOCID};

pub struct PrimaryKeyPostingIterator {
    docid: DocId,
}

impl PrimaryKeyPostingIterator {
    pub fn new(docid: DocId) -> Self {
        Self { docid }
    }
}

impl PostingIterator for PrimaryKeyPostingIterator {
    fn seek(&mut self, docid: crate::DocId) -> io::Result<crate::DocId> {
        if docid <= self.docid {
            Ok(self.docid)
        } else {
            Ok(END_DOCID)
        }
    }
}
