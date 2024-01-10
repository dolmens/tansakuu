use super::AcqRelU64;

pub struct Bitset {
    data: Box<[AcqRelU64]>,
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / 64, index % 64)
}

impl Bitset {
    pub fn with_capacity(capacity: usize) -> Self {
        let len = (capacity + 63) / 64;
        let vec: Vec<_> = (0..len).map(|_| AcqRelU64::new(0)).collect();
        let data = vec.into_boxed_slice();
        Self { data }
    }

    pub unsafe fn insert(&self, index: usize) {
        if index < self.capacity() {
            let (quot, rem) = quot_and_rem(index);
            let mut slot = self.data[quot].load();
            slot |= 1 << rem;
            self.data[quot].store(slot);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.iter().all(|a| a.load() == 0)
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use super::Bitset;

    #[test]
    fn test_simple() {
        let capacity = 129;
        let bitset = Bitset::with_capacity(capacity);

        for i in 0..capacity {
            assert!(!bitset.contains(i));
        }

        // Overflow
        assert!(!bitset.contains(capacity));

        unsafe {
            bitset.insert(2);
        }
        for i in 0..capacity {
            if i == 2 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        unsafe {
            bitset.insert(0);
        }
        for i in 0..capacity {
            if i == 0 || i == 2 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        unsafe {
            bitset.insert(64);
            bitset.insert(capacity - 1);
        }
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
        let bitset = Arc::new(Bitset::with_capacity(capacity));

        let bitset1 = bitset.clone();
        let t1 = thread::spawn(move || {
            while !bitset1.contains(capacity - 1) {
                thread::sleep(Duration::from_millis(1));
            }
            for i in 0..capacity {
                if i == 0 || i == 2 || i == 64 || i == capacity - 1 {
                    assert!(bitset1.contains(i));
                } else {
                    assert!(!bitset1.contains(i));
                }
            }
        });

        for i in 0..capacity {
            assert!(!bitset.contains(i));
        }

        // Overflow
        assert!(!bitset.contains(capacity));

        unsafe {
            bitset.insert(2);
        }
        for i in 0..capacity {
            if i == 2 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        unsafe {
            bitset.insert(0);
        }
        for i in 0..capacity {
            if i == 0 || i == 2 {
                assert!(bitset.contains(i));
            } else {
                assert!(!bitset.contains(i));
            }
        }

        unsafe {
            bitset.insert(64);
            bitset.insert(capacity - 1);
        }
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
