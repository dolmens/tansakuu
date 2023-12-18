use crate::{index::PostingIterator, DocId, END_DOCID};

pub struct UniqueKeyPostingIterator {
    docid: DocId,
}

impl UniqueKeyPostingIterator {
    pub fn new(docid: DocId) -> Self {
        Self { docid }
    }
}

impl PostingIterator for UniqueKeyPostingIterator {
    fn seek(&mut self, docid: crate::DocId) -> crate::DocId {
        if docid <= self.docid {
            self.docid
        } else {
            END_DOCID
        }
    }
}
