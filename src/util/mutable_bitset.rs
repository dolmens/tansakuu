use super::immutable_bitset::ImmutableBitset;

type Word = u64;
const BITS: usize = std::mem::size_of::<Word>() * 8;

#[derive(Clone)]
pub struct MutableBitset {
    data: Vec<Word>,
}

impl Into<ImmutableBitset> for MutableBitset {
    fn into(self) -> ImmutableBitset {
        ImmutableBitset::new(&self.data)
    }
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / BITS, index % BITS)
}

impl MutableBitset {
    pub fn new(data: Vec<Word>) -> Self {
        Self { data }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let len = (capacity + BITS - 1) / BITS;
        let data: Vec<Word> = (0..len).map(|_| 0).collect();
        Self { data }
    }

    pub fn copy_data_at(&mut self, data: &[Word], bit_offset: usize, bit_count: usize) {
        let (mut word_offset, bit_offset) = (bit_offset / BITS, bit_offset % BITS);
        let bit_remain_in_dst = BITS - bit_offset;
        let word_count = (bit_count + BITS - 1) / BITS;
        debug_assert!(word_count <= data.len());
        for i in 0..word_count - 1 {
            let value = data[i];
            self.data[word_offset] |= value << bit_offset;
            word_offset += 1;
            if bit_offset != 0 {
                self.data[word_offset] |= value >> bit_remain_in_dst;
            }
        }

        let bit_remain_in_src = bit_count - BITS * (word_count - 1);
        let value = data[word_count - 1];
        if bit_remain_in_src <= bit_remain_in_dst {
            let value =
                (value << (BITS - bit_remain_in_src)) >> (bit_remain_in_dst - bit_remain_in_src);
            self.data[word_offset] |= value;
        } else {
            self.data[word_offset] |= value << bit_offset;
            word_offset += 1;
            let value = (value << (BITS - bit_remain_in_src))
                >> (BITS - bit_remain_in_src + bit_remain_in_dst);
            self.data[word_offset] |= value;
        }
    }

    pub fn insert(&mut self, index: usize) {
        if index < self.capacity() {
            let (quot, rem) = quot_and_rem(index);
            self.data[quot] |= 1 << rem;
        }
    }

    pub fn contains(&self, index: usize) -> bool {
        if index < self.capacity() {
            let (quot, rem) = quot_and_rem(index);
            let slot = self.data[quot];
            slot & (1 << rem) != 0
        } else {
            false
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.len() * BITS
    }

    pub fn data(&self) -> &[Word] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::MutableBitset;

    #[test]
    fn test_copy_data_at() {
        let mut s1 = MutableBitset::with_capacity(64 * 3);
        let mut s2 = MutableBitset::with_capacity(64 * 2);
        s2.insert(0);
        assert!(s2.contains(0));
        s1.copy_data_at(s2.data(), 1, 64 * 2);
        assert!(s1.contains(1));
    }
}
