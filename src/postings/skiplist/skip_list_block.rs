use crate::{DocId, TermFreq, SKIPLIST_BLOCK_LEN};

pub struct SkipListBlock {
    pub len: usize,
    pub docids: [DocId; SKIPLIST_BLOCK_LEN],
    pub offsets: [u32; SKIPLIST_BLOCK_LEN],
    pub termfreqs: Option<Box<[TermFreq]>>,
}
