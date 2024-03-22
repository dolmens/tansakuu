use crate::util::ImmutableBitset;

pub struct ImmutableBitsetPostingIterator<'a> {
    bitset: &'a ImmutableBitset,
}
