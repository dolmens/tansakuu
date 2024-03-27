use super::{buffer::Buffer, bytes::Bytes};

type Word = u64;
const BITS: usize = std::mem::size_of::<Word>() * 8;

#[derive(Clone)]
pub struct ImmutableBitset {
    data: Buffer,
}

pub struct ImmutableBitsetIter<'a> {
    index: usize,
    bitset: &'a ImmutableBitset,
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / BITS, index % BITS)
}

impl ImmutableBitset {
    pub fn from_bytes(bytes: Bytes) -> Self {
        let data = Buffer::from_bytes(bytes);
        assert!(data.typed_data::<Word>().len() > 0);
        Self { data }
    }

    pub fn from_vec(vec: Vec<Word>) -> Self {
        let data = Buffer::from_vec(vec);
        Self { data }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let len = (capacity + BITS - 1) / BITS;
        let vec: Vec<u64> = (0..len).map(|_| 0).collect();
        let data = Buffer::from_vec(vec);
        Self { data }
    }

    pub fn contains(&self, index: usize) -> bool {
        if index < self.capacity() {
            let (quot, rem) = quot_and_rem(index);
            let slot = self.data()[quot];
            slot & (1 << rem) != 0
        } else {
            false
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.len() * 8
    }

    pub fn data(&self) -> &[Word] {
        let data = self.data.as_ptr() as *const Word;
        let len = self.data.len() / std::mem::size_of::<Word>();
        unsafe { std::slice::from_raw_parts(data, len) }
    }

    pub fn word(&self, pos: usize) -> Word {
        self.data().get(pos).copied().unwrap_or_default()
    }

    pub fn iter(&self) -> ImmutableBitsetIter {
        ImmutableBitsetIter {
            index: 0,
            bitset: self,
        }
    }

    pub fn count_ones(&self) -> usize {
        self.data().iter().map(|w| w.count_ones() as usize).sum()
    }
}

impl<'a> Iterator for ImmutableBitsetIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.bitset.capacity() {
            let index = self.index;
            self.index += 1;
            if self.bitset.contains(index) {
                return Some(index);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::ImmutableBitset;

    #[test]
    fn test_iter() {
        let vec = vec![1, 2];
        let bitset = ImmutableBitset::from_vec(vec);
        let expect = vec![0, 65];
        let got: Vec<_> = bitset.iter().collect();
        assert_eq!(got, expect);
    }
}
