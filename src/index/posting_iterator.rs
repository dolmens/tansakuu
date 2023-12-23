use crate::DocId;

pub trait PostingIterator {
    fn seek(&mut self, docid: DocId) -> Option<DocId>;
}
