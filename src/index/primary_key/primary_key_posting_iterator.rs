use crate::{index::PostingIterator, DocId};

pub struct PrimaryKeyPostingIterator {
    docid: DocId,
}

impl PrimaryKeyPostingIterator {
    pub fn new(docid: DocId) -> Self {
        Self { docid }
    }
}

impl PostingIterator for PrimaryKeyPostingIterator {
    fn seek(&mut self, docid: crate::DocId) -> Option<crate::DocId> {
        if docid <= self.docid {
            Some(self.docid)
        } else {
            None
        }
    }
}
