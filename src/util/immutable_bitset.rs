use std::sync::Arc;

type Word = u64;
const BITS: usize = std::mem::size_of::<Word>() * 8;

#[derive(Clone)]
pub struct ImmutableBitset {
    data: Arc<[Word]>,
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / BITS, index % BITS)
}

impl ImmutableBitset {
    pub fn new(data: &[Word]) -> Self {
        Self { data: data.into() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let len = (capacity + BITS - 1) / BITS;
        let vec: Vec<_> = (0..len).map(|_| 0).collect();
        let data = Arc::from(vec.into_boxed_slice());
        Self { data }
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
