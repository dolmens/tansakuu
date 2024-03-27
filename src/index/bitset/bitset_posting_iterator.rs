use crate::{index::PostingIterator, DocId, END_DOCID, INVALID_DOCID};

use super::BitsetSegmentPosting;

pub struct BitsetPostingIterator<'a> {
    current_docid: DocId,
    word_pos: usize,
    current_word: u64,
    segment_cursor: usize,
    postings: Vec<BitsetSegmentPosting<'a>>,
}

impl<'a> BitsetPostingIterator<'a> {
    pub fn new(postings: Vec<BitsetSegmentPosting<'a>>) -> Self {
        Self {
            current_docid: INVALID_DOCID,
            word_pos: usize::MAX,
            current_word: 0,
            segment_cursor: 0,
            postings,
        }
    }
}

const BITS: usize = 64;

impl<'a> PostingIterator for BitsetPostingIterator<'a> {
    fn seek(&mut self, docid: crate::DocId) -> std::io::Result<crate::DocId> {
        let docid = if docid < 0 { 0 } else { docid };
        if docid <= self.current_docid {
            return Ok(self.current_docid);
        }

        let mut docid = docid;
        loop {
            let current_segment = &self.postings[self.segment_cursor];
            if docid >= current_segment.base_docid + (current_segment.doc_count as DocId) {
                self.word_pos = usize::MAX;
                self.segment_cursor += 1;
                if self.segment_cursor == self.postings.len() {
                    self.current_docid = END_DOCID;
                    return Ok(END_DOCID);
                }
                continue;
            }

            let docid_in_segment = docid - current_segment.base_docid;

            if ((docid_in_segment as usize) / BITS) != self.word_pos {
                self.word_pos = (docid_in_segment as usize) / BITS;
                self.current_word = current_segment.bitset.load_word(self.word_pos);
            }

            if self.current_word & (1 << ((docid_in_segment as usize) % BITS)) != 0 {
                self.current_docid = docid;
                return Ok(docid);
            }

            docid += 1;
        }
    }
}
