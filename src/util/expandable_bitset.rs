use std::{
    alloc::{handle_alloc_error, Layout},
    ptr::{self, NonNull},
    sync::Arc,
};

use crate::arena::{Arena, ArenaGuard, BumpArena};

use super::atomic::{AcqRelU64, AcqRelUsize, RelaxedAtomicPtr};

type AtomicWord = AcqRelU64;
const BITS: usize = std::mem::size_of::<AtomicWord>() * 8;

pub struct ExpandableBitsetWriter<A: Arena = BumpArena> {
    data: Arc<ExpandableBitsetData>,
    arena: A,
}

#[derive(Clone)]
pub struct ExpandableBitset {
    data: Arc<ExpandableBitsetData>,
}

struct ExpandableBitsetData {
    data: RelaxedAtomicPtr<AtomicWord>,
    capacity: AcqRelUsize,
    _arena_guard: ArenaGuard,
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / BITS, index % BITS)
}

impl ExpandableBitsetWriter<BumpArena> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_in(capacity, BumpArena::new())
    }
}

impl<A: Arena> ExpandableBitsetWriter<A> {
    pub fn with_capacity_in(capacity: usize, arena: A) -> Self {
        let len = (capacity + BITS - 1) / BITS;
        let capacity = len * BITS;
        let layout = Layout::array::<AtomicWord>(len).unwrap();
        let data = arena
            .allocate(layout)
            .unwrap_or_else(|_| handle_alloc_error(layout))
            .cast::<AtomicWord>();
        for i in 0..len {
            unsafe {
                ptr::write(data.as_ptr().add(i), AtomicWord::new(0));
            }
        }
        let data = Arc::new(ExpandableBitsetData::new(capacity, data, arena.guard()));

        Self { data, arena }
    }

    pub fn bitset(&self) -> ExpandableBitset {
        ExpandableBitset {
            data: self.data.clone(),
        }
    }

    pub fn insert(&mut self, index: usize) {
        if index >= self.capacity() {
            self.expand(index);
        }
        let data = self.data_ptr();
        let (quot, rem) = quot_and_rem(index);
        let slot = unsafe { &*data.as_ptr().add(quot) };
        slot.store(slot.load() | (1 << rem));
    }

    fn expand(&mut self, index: usize) {
        let capacity = self.capacity();
        let len = capacity / BITS;
        let mut next_len = len;
        let add_len = std::cmp::max(len / 2, 1);
        loop {
            next_len += add_len;
            if next_len * BITS > index {
                break;
            }
        }

        let old_data = self.data_ptr();

        let layout = Layout::array::<AtomicWord>(next_len).unwrap();
        let data = self
            .arena
            .allocate(layout)
            .unwrap_or_else(|_| handle_alloc_error(layout))
            .cast::<AtomicWord>();

        for i in 0..len {
            unsafe {
                ptr::write(data.as_ptr().add(i), ptr::read(old_data.as_ptr().add(i)));
            }
        }

        for i in len..next_len {
            unsafe {
                ptr::write(data.as_ptr().add(i), AtomicWord::new(0));
            }
        }

        self.data.data.store(data.as_ptr());
        self.data.capacity.store(next_len * BITS);
    }

    pub fn contains(&self, index: usize) -> bool {
        self.data.contains(index)
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn data_ptr(&self) -> NonNull<AtomicWord> {
        self.data.data_ptr()
    }
}

impl ExpandableBitsetData {
    fn new(capacity: usize, data: NonNull<AtomicWord>, arena_guard: ArenaGuard) -> Self {
        Self {
            data: RelaxedAtomicPtr::new(data.as_ptr()),
            capacity: AcqRelUsize::new(capacity),
            _arena_guard: arena_guard,
        }
    }

    fn capacity(&self) -> usize {
        self.capacity.load()
    }

    fn data_ptr(&self) -> NonNull<AtomicWord> {
        unsafe { NonNull::new_unchecked(self.data.load()) }
    }

    fn data(&self) -> &[AtomicWord] {
        let capacity = self.capacity();
        let data = self.data_ptr();
        unsafe { std::slice::from_raw_parts(data.as_ptr(), capacity / BITS) }
    }

    fn contains(&self, index: usize) -> bool {
        if index < self.capacity() {
            let (quot, rem) = quot_and_rem(index);
            let data = self.data_ptr();
            let slot = unsafe { &*data.as_ptr().add(quot) };
            (slot.load() & (1 << rem)) != 0
        } else {
            false
        }
    }
}

impl ExpandableBitset {
    pub fn contains(&self, index: usize) -> bool {
        self.data.contains(index)
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn data(&self) -> &[AtomicWord] {
        self.data.data()
    }

    pub fn as_loaded_words(&self) -> impl Iterator<Item = u64> + '_ {
        self.data.data().iter().map(|w| w.load())
    }

    pub fn count_ones(&self) -> usize {
        self.data()
            .iter()
            .map(|w| w.load().count_ones() as usize)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::BITS;
    use crate::util::expandable_bitset::ExpandableBitsetWriter;

    #[test]
    fn test_bits() {
        assert_eq!(BITS, 64);
    }

    #[test]
    fn test_simple() {
        let capacity = 129;
        let mut writer = ExpandableBitsetWriter::with_capacity(capacity);
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
    fn test_expand() {
        let capacity = 1;
        let mut writer = ExpandableBitsetWriter::with_capacity(capacity);
        let bitset = writer.bitset();
        let capacity = BITS;
        assert_eq!(bitset.capacity(), capacity);

        writer.insert(BITS - 1);
        assert_eq!(bitset.capacity(), capacity);
        assert!(bitset.contains(BITS - 1));

        writer.insert(BITS);
        let capacity = BITS * 2;
        assert_eq!(bitset.capacity(), capacity);
        assert!(bitset.contains(BITS - 1));
        assert!(bitset.contains(BITS));

        writer.insert(BITS * 4 - 1);
        let capacity = BITS * 4;
        assert_eq!(bitset.capacity(), capacity);
        assert!(bitset.contains(BITS - 1));
        assert!(bitset.contains(BITS));
        assert!(bitset.contains(BITS * 4 - 1));

        writer.insert(BITS * 4);
        let capacity = BITS * 6;
        assert_eq!(bitset.capacity(), capacity);
        assert!(bitset.contains(BITS - 1));
        assert!(bitset.contains(BITS));
        assert!(bitset.contains(BITS * 4 - 1));
        assert!(bitset.contains(BITS * 4));
    }

    #[test]
    fn test_expand_multithread() {
        let capacity = 1;
        let mut writer = ExpandableBitsetWriter::with_capacity(capacity);
        let bitset = writer.bitset();
        let reader = bitset.clone();

        let th = thread::spawn(move || {
            loop {
                if reader.capacity() < BITS * 6 || !reader.contains(BITS * 4) {
                    thread::yield_now();
                } else {
                    break;
                }
            }
            assert!(reader.contains(BITS - 1));
            assert!(reader.contains(BITS));
            assert!(reader.contains(BITS * 4 - 1));
        });

        let capacity = BITS;
        assert_eq!(bitset.capacity(), capacity);

        writer.insert(BITS - 1);
        assert_eq!(bitset.capacity(), capacity);
        assert!(bitset.contains(BITS - 1));

        writer.insert(BITS);
        let capacity = BITS * 2;
        assert_eq!(bitset.capacity(), capacity);
        assert!(bitset.contains(BITS - 1));
        assert!(bitset.contains(BITS));

        writer.insert(BITS * 4 - 1);
        let capacity = BITS * 4;
        assert_eq!(bitset.capacity(), capacity);
        assert!(bitset.contains(BITS - 1));
        assert!(bitset.contains(BITS));
        assert!(bitset.contains(BITS * 4 - 1));

        writer.insert(BITS * 4);
        let capacity = BITS * 6;
        assert_eq!(bitset.capacity(), capacity);
        assert!(bitset.contains(BITS - 1));
        assert!(bitset.contains(BITS));
        assert!(bitset.contains(BITS * 4 - 1));
        assert!(bitset.contains(BITS * 4));

        th.join().unwrap();
    }

    #[test]
    fn test_multithreads() {
        let capacity = 129;
        let mut writer = ExpandableBitsetWriter::with_capacity(capacity);
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
