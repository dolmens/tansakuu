use super::{buffer::Buffer, bytes::Bytes};

type Word = u64;
const BITS: usize = std::mem::size_of::<Word>() * 8;

#[derive(Clone)]
pub struct ImmutableBitset {
    data: Buffer,
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
        self.data.len() * BITS
    }

    pub fn data(&self) -> &[Word] {
        let data = self.data.as_ptr() as *const Word;
        let len = self.data.len() / std::mem::size_of::<Word>();
        unsafe { std::slice::from_raw_parts(data, len) }
    }

    pub fn count_ones(&self) -> usize {
        self.data().iter().map(|w| w.count_ones() as usize).sum()
    }
}
