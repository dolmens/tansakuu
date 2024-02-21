use std::sync::Arc;

use super::atomic::AcqRelU64;

type AtomicWord = AcqRelU64;
const BITS: usize = std::mem::size_of::<AtomicWord>() * 8;

pub struct FixedBitsetWriter {
    data: Arc<[AtomicWord]>,
}

#[derive(Clone)]
pub struct FixedBitset {
    data: Arc<[AtomicWord]>,
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / BITS, index % BITS)
}

impl FixedBitsetWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        let len = (capacity + BITS - 1) / BITS;
        let vec: Vec<_> = (0..len).map(|_| AtomicWord::new(0)).collect();
        let data = Arc::from(vec.into_boxed_slice());
        Self { data }
    }

    pub fn bitset(&self) -> FixedBitset {
        FixedBitset {
            data: self.data.clone(),
        }
    }

    pub fn insert(&mut self, index: usize) {
        if index < self.capacity() {
            let (quot, rem) = quot_and_rem(index);
            let mut slot = self.data[quot].load();
            slot |= 1 << rem;
            self.data[quot].store(slot);
        }
    }

    pub fn union_with(&mut self, other: &FixedBitset) {
        if self.capacity() != other.capacity() {
            return;
        }

        for (l, r) in self.data.iter().zip(other.data.iter()) {
            l.store(l.load() | r.load());
        }
    }

    pub fn contains(&self, index: usize) -> bool {
        if index < self.capacity() {
            let (quot, rem) = quot_and_rem(index);
            let slot = self.data[quot].load();
            slot & (1 << rem) != 0
        } else {
            false
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.len() * BITS
    }
}

impl FixedBitset {
    pub fn contains(&self, index: usize) -> bool {
        if index < self.capacity() {
            let (quot, rem) = quot_and_rem(index);
            let slot = self.data[quot].load();
            slot & (1 << rem) != 0
        } else {
            false
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.len() * BITS
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::BITS;
    use crate::util::fixed_bitset::FixedBitsetWriter;

    #[test]
    fn test_bits() {
        assert_eq!(BITS, 64);
    }

    #[test]
    fn test_simple() {
        let capacity = 129;
        let mut writer = FixedBitsetWriter::with_capacity(capacity);
        let bitset = writer.bitset();

        for i in 0..capacity {
            assert!(!bitset.contains(i));
        }

        // Overflow
        assert!(!bitset.contains(capacity));

        writer.insert(2);

        for i in 0..capacity {
            if i == 2 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        writer.insert(0);
        for i in 0..capacity {
            if i == 0 || i == 2 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        writer.insert(64);
        writer.insert(capacity - 1);
        for i in 0..capacity {
            if i == 0 || i == 2 || i == 64 || i == capacity - 1 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }
    }

    #[test]
    fn test_union_with() {
        let mut w1 = FixedBitsetWriter::with_capacity(BITS * 3);
        let mut w2 = FixedBitsetWriter::with_capacity(BITS * 3);

        w1.insert(0);
        w1.insert(1);
        w1.insert(63);
        w1.insert(150);

        w2.insert(1);
        w2.insert(60);

        w1.union_with(&w2.bitset());
        let bitset = w1.bitset();

        assert!(bitset.contains(0));
        assert!(bitset.contains(1));
        assert!(bitset.contains(60));
        assert!(bitset.contains(63));
        assert!(bitset.contains(150));
    }

    #[test]
    fn test_multithreads() {
        let capacity = 129;
        let mut writer = FixedBitsetWriter::with_capacity(capacity);
        let bitset = writer.bitset();

        let reader = bitset.clone();
        let t1 = thread::spawn(move || {
            while !reader.contains(capacity - 1) {
                thread::sleep(Duration::from_millis(1));
            }
            for i in 0..capacity {
                if i == 0 || i == 2 || i == 64 || i == capacity - 1 {
                    assert!(reader.contains(i));
                } else {
                    assert!(!reader.contains(i));
                }
            }
        });

        for i in 0..capacity {
            assert!(!bitset.contains(i));
        }

        // Overflow
        assert!(!writer.contains(capacity));

        writer.insert(2);
        for i in 0..capacity {
            if i == 2 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        writer.insert(0);
        for i in 0..capacity {
            if i == 0 || i == 2 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        writer.insert(64);
        writer.insert(capacity - 1);
        for i in 0..capacity {
            if i == 0 || i == 2 || i == 64 || i == capacity - 1 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        t1.join().unwrap();
    }
}
