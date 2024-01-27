use std::sync::Arc;

use super::AcqRelU64;

pub struct BitsetWriter {
    data: Arc<[AcqRelU64]>,
}

#[derive(Clone)]
pub struct Bitset {
    data: Arc<[AcqRelU64]>,
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / 64, index % 64)
}

impl BitsetWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        let len = (capacity + 63) / 64;
        let vec: Vec<_> = (0..len).map(|_| AcqRelU64::new(0)).collect();
        let data = Arc::from(vec.into_boxed_slice());
        Self { data }
    }

    pub fn bitset(&self) -> Bitset {
        Bitset {
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
        self.data.len() * 64
    }
}

impl Bitset {
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
        self.data.len() * 64
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use crate::util::bitset::BitsetWriter;

    #[test]
    fn test_simple() {
        let capacity = 129;
        let mut writer = BitsetWriter::with_capacity(capacity);
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
    fn test_multithreads() {
        let capacity = 129;
        let mut writer = BitsetWriter::with_capacity(capacity);
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
