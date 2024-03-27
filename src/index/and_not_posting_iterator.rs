use crate::{DocId, END_DOCID, INVALID_DOCID};

use super::PostingIterator;

pub struct AndNotPostingIterator<I: PostingIterator, E: PostingIterator> {
    current_docid: DocId,
    include_posting_iterator: I,
    exclude_posting_iterator: E,
}

impl<I: PostingIterator, E: PostingIterator> AndNotPostingIterator<I, E> {
    pub fn new(include_posting_iterator: I, exclude_posting_iterator: E) -> Self {
        Self {
            current_docid: INVALID_DOCID,
            include_posting_iterator,
            exclude_posting_iterator,
        }
    }
}

impl<I: PostingIterator, E: PostingIterator> PostingIterator for AndNotPostingIterator<I, E> {
    fn seek(&mut self, docid: crate::DocId) -> std::io::Result<crate::DocId> {
        let docid = if docid < 0 { 0 } else { docid };
        if docid <= self.current_docid {
            return Ok(self.current_docid);
        }

        let mut docid = docid;
        loop {
            docid = self.include_posting_iterator.seek(docid)?;
            if docid == END_DOCID {
                self.current_docid = END_DOCID;
                return Ok(END_DOCID);
            }
            if self.exclude_posting_iterator.seek(docid)? != docid {
                self.current_docid = docid;
                return Ok(docid);
            }
            docid += 1;
        }
    }
}
