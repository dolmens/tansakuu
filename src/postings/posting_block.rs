use crate::{DocId, FieldMask, TermFreq, POSTING_BLOCK_LEN};

use super::PostingFormat;

pub struct PostingBlock {
    pub len: usize,
    pub docids: [DocId; POSTING_BLOCK_LEN],
    pub termfreqs: Option<Box<[TermFreq]>>,
    pub fieldmasks: Option<Box<[FieldMask]>>,
}

impl PostingBlock {
    pub fn new(doc_list_format: &PostingFormat) -> Self {
        let termfreqs = if doc_list_format.has_tflist() {
            Some(
                std::iter::repeat(0)
                    .take(POSTING_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };
        let fieldmasks = if doc_list_format.has_fieldmask() {
            Some(
                std::iter::repeat(0)
                    .take(POSTING_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            len: 0,
            docids: [0; POSTING_BLOCK_LEN],
            termfreqs,
            fieldmasks,
        }
    }

    pub fn first_docid(&self) -> DocId {
        assert!(self.len > 0);
        self.docids[0]
    }

    pub fn last_docid(&self) -> DocId {
        self.docids[self.len - 1]
    }

    pub fn decode_docids(&mut self, last_docid: DocId) {
        self.docids[0..self.len]
            .iter_mut()
            .fold(last_docid, |acc, elem| {
                *elem += acc;
                *elem
            });
    }
}
