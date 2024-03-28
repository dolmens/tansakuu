use super::{buffer::Buffer, bytes::Bytes};

#[derive(Clone)]
pub struct ImmutableBitset8 {
    data: Buffer,
}

pub struct ImmutableBitset8Iter<'a> {
    index: usize,
    bitset: &'a ImmutableBitset8,
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / 8, index % 8)
}

impl ImmutableBitset8 {
    pub fn from_bytes(bytes: Bytes) -> Self {
        let data = Buffer::from_bytes(bytes);
        Self { data }
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        let data = Buffer::from_vec(vec);
        Self { data }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let len = (capacity + 8 - 1) / 8;
        let vec: Vec<u8> = vec![0; len];
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

    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn word(&self, word_pos: usize) -> u64 {
        let data = self.data();
        let pos = word_pos * 8;
        data.iter()
            .skip(pos)
            .take(8)
            .enumerate()
            .fold(0, |acc, (i, &b)| ((b as u64) << (i * 8)) | acc)
    }

    pub fn iter(&self) -> ImmutableBitset8Iter {
        ImmutableBitset8Iter {
            index: 0,
            bitset: self,
        }
    }

    pub fn count_ones(&self) -> usize {
        self.data().iter().map(|b| b.count_ones() as usize).sum()
    }
}

impl<'a> Iterator for ImmutableBitset8Iter<'a> {
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
    use super::ImmutableBitset8;

    #[test]
    fn test_iter() {
        let vec = vec![1, 2];
        let bitset = ImmutableBitset8::from_vec(vec);
        assert_eq!(bitset.capacity(), 16);

        let expect_data: Vec<u8> = vec![1, 2];
        let got_data: Vec<u8> = bitset.data().iter().copied().collect();
        assert_eq!(got_data, expect_data);

        let expect_index = vec![0, 9];
        let got_index: Vec<_> = bitset.iter().collect();
        assert_eq!(got_index, expect_index);
    }
}
