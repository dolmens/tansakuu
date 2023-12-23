use crate::{index::PostingIterator, DocId};

pub struct UniqueKeyPostingIterator {
    docid: DocId,
}

impl UniqueKeyPostingIterator {
    pub fn new(docid: DocId) -> Self {
        Self { docid }
    }
}

impl PostingIterator for UniqueKeyPostingIterator {
    fn seek(&mut self, docid: crate::DocId) -> Option<crate::DocId> {
        if docid <= self.docid {
            Some(self.docid)
        } else {
            None
        }
    }
}
