use crate::{
    postings::{BufferedByteSlice, DocSkipListFormat},
    DocId, TermFreq, DOC_BLOCK_LEN,
};

pub struct BufferedSkipListWriter {
    format: DocSkipListFormat,
    buffered_byte_slice: BufferedByteSlice,
}

impl BufferedSkipListWriter {
    pub fn new(format: DocSkipListFormat) -> Self {
        let buffered_byte_slice =
            BufferedByteSlice::new(format.value_items().clone(), DOC_BLOCK_LEN);
        Self {
            format,
            buffered_byte_slice,
        }
    }

    pub fn add_with_offset(&mut self, docid: DocId, offset: usize) {
        debug_assert!(!self.format.has_tflist());
        self.buffered_byte_slice.push(0, docid);
        self.buffered_byte_slice.push(1, offset);
        self.buffered_byte_slice.end_push();
    }

    pub fn add_with_tf_and_offset(&mut self, docid: DocId, tf: TermFreq, offset: usize) {
        debug_assert!(self.format.has_tflist());
        self.buffered_byte_slice.push(0, docid);
        self.buffered_byte_slice.push(1, tf);
        self.buffered_byte_slice.push(2, offset);
        self.buffered_byte_slice.end_push();
    }
}
