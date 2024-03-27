use crate::{table::SegmentRegistry, DocId, END_DOCID, INVALID_DOCID};

use super::PostingIterator;

pub struct AllDocsPostingIterator {
    current_docid: DocId,
    segment_cursor: usize,
    segment_registry: SegmentRegistry,
}

impl AllDocsPostingIterator {
    pub fn new(segment_registry: SegmentRegistry) -> Self {
        Self {
            current_docid: INVALID_DOCID,
            segment_cursor: 0,
            segment_registry,
        }
    }
}

impl PostingIterator for AllDocsPostingIterator {
    fn seek(&mut self, docid: crate::DocId) -> std::io::Result<crate::DocId> {
        let docid = if docid < 0 { 0 } else { docid };
        if docid <= self.current_docid {
            return Ok(self.current_docid);
        }

        loop {
            if let Some(segment_cursor) = self
                .segment_registry
                .locate_segment_from(docid, self.segment_cursor)
            {
                self.segment_cursor = segment_cursor;
                self.current_docid = docid;
                return Ok(docid);
            } else {
                self.current_docid = END_DOCID;
                return Ok(END_DOCID);
            }
        }
    }
}
