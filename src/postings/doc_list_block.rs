use crate::{DocId, DocId32, DOC_LIST_BLOCK_LEN};

use super::DocListFormat;

pub struct DocListBlock {
    pub base_docid: DocId,
    pub last_docid: DocId,
    pub base_ttf: u64,
    pub len: usize,
    pub docids: [DocId32; DOC_LIST_BLOCK_LEN],
    pub termfreqs: Option<Box<[u32]>>,
    pub fieldmasks: Option<Box<[u8]>>,
}

impl DocListBlock {
    pub fn new(doc_list_format: &DocListFormat) -> Self {
        let termfreqs = if doc_list_format.has_tflist() {
            Some(
                std::iter::repeat(0)
                    .take(DOC_LIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };
        let fieldmasks = if doc_list_format.has_fieldmask() {
            Some(
                std::iter::repeat(0)
                    .take(DOC_LIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            base_docid: 0,
            last_docid: 0,
            base_ttf: 0,
            len: 0,
            docids: [0; DOC_LIST_BLOCK_LEN],
            termfreqs,
            fieldmasks,
        }
    }

    pub fn new_tf_buf(&self) -> Box<[u32]> {
        unimplemented!()
    }

    #[cfg(test)]
    pub fn decode_docids(&self, base_docid: DocId) -> Vec<DocId> {
        self.docids[0..self.len]
            .iter()
            .scan(base_docid, |acc, &elem| {
                *acc += elem as DocId;
                Some(*acc)
            })
            .collect()
    }
}
