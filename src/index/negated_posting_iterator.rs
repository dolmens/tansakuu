use crate::{table::SegmentRegistry, DocId, END_DOCID, INVALID_DOCID};

use super::PostingIterator;

pub struct NegatedPostingIterator<I: PostingIterator> {
    current_docid: DocId,
    segment_cursor: usize,
    segment_registry: SegmentRegistry,
    posting_iterator: I,
}

impl<I: PostingIterator> NegatedPostingIterator<I> {
    pub fn new(segment_registry: SegmentRegistry, posting_iterator: I) -> Self {
        Self {
            current_docid: INVALID_DOCID,
            segment_cursor: 0,
            segment_registry,
            posting_iterator,
        }
    }
}

impl<I: PostingIterator> PostingIterator for NegatedPostingIterator<I> {
    fn seek(&mut self, docid: crate::DocId) -> std::io::Result<crate::DocId> {
        let docid = if docid < 0 { 0 } else { docid };
        if docid <= self.current_docid {
            return Ok(self.current_docid);
        }

        let mut docid = docid;
        loop {
            if let Some(segment_cursor) = self
                .segment_registry
                .locate_segment_from(docid, self.segment_cursor)
            {
                self.segment_cursor = segment_cursor;
                let segment_end_docid =
                    self.segment_registry.segment_end_docid(self.segment_cursor);
                while docid < segment_end_docid {
                    if self.posting_iterator.seek(docid)? != docid {
                        self.current_docid = docid;
                        return Ok(docid);
                    }
                    docid += 1;
                }
            } else {
                self.current_docid = END_DOCID;
                return Ok(END_DOCID);
            }
        }
    }
}
