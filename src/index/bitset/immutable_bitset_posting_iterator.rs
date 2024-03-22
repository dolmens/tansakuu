use crate::util::ImmutableBitset;

pub struct ImmutableBitsetPostingIterator<'a> {
    _bitset: &'a ImmutableBitset,
}
