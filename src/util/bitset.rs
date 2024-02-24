use super::{fixed_size_bitset::FixedSizeBitset, immutable_bitset::ImmutableBitset};

#[derive(Clone)]
pub enum Bitset {
    Immutable(ImmutableBitset),
    FixedSize(FixedSizeBitset),
}

impl From<ImmutableBitset> for Bitset {
    fn from(value: ImmutableBitset) -> Self {
        Self::Immutable(value)
    }
}

impl From<FixedSizeBitset> for Bitset {
    fn from(value: FixedSizeBitset) -> Self {
        Self::FixedSize(value)
    }
}

impl Bitset {
    pub fn contains(&self, index: usize) -> bool {
        match self {
            Bitset::Immutable(bitset) => bitset.contains(index),
            Bitset::FixedSize(bitset) => bitset.contains(index),
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            Bitset::Immutable(bitset) => bitset.capacity(),
            Bitset::FixedSize(bitset) => bitset.capacity(),
        }
    }
}
