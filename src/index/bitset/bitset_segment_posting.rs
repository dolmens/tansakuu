use crate::{
    util::{ExpandableBitset, ImmutableBitset},
    DocId,
};

pub struct BitsetSegmentPosting<'a> {
    pub base_docid: DocId,
    pub doc_count: usize,
    pub bitset: BitsetPostingVariant<'a>,
}

pub enum BitsetPostingVariant<'a> {
    Immutable(&'a ImmutableBitset),
    Mutable(&'a ExpandableBitset),
}

impl<'a> BitsetSegmentPosting<'a> {
    pub fn new_immutable(base_docid: DocId, doc_count: usize, bitset: &'a ImmutableBitset) -> Self {
        Self {
            base_docid,
            doc_count,
            bitset: BitsetPostingVariant::Immutable(bitset),
        }
    }

    pub fn new_mutable(base_docid: DocId, doc_count: usize, bitset: &'a ExpandableBitset) -> Self {
        Self {
            base_docid,
            doc_count,
            bitset: BitsetPostingVariant::Mutable(bitset),
        }
    }
}

impl<'a> BitsetPostingVariant<'a> {
    pub fn load_word(&self, pos: usize) -> u64 {
        match self {
            Self::Immutable(bitset) => bitset.word(pos),
            Self::Mutable(bitset) => bitset.word(pos),
        }
    }
}
