use crate::index::{PostingIterator, SegmentPosting};

pub struct BufferedPostingIterator {
    segment_cursor: usize,
    segment_postings: Vec<SegmentPosting>,
}

impl BufferedPostingIterator {
    pub fn new(segment_postings: Vec<SegmentPosting>) -> Self {
        Self {
            segment_cursor: 0,
            segment_postings,
        }
    }
}

impl PostingIterator for BufferedPostingIterator {
    fn seek(&mut self, docid: crate::DocId) -> Option<crate::DocId> {
        while self.segment_cursor < self.segment_postings.len() {
            let seeked = self.segment_postings[self.segment_cursor].seek(docid);
            if seeked.is_some() {
                return seeked;
            }
            self.segment_cursor += 1;
        }
        None
    }
}
