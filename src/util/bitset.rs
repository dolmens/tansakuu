use std::sync::Arc;

use super::{
    atomic::{AcqRelU64, AcqRelUsize, RelaxedAtomicPtr},
    LinkedList, LinkedListWriter,
};

type AtomicWord = AcqRelU64;
const BITS: usize = std::mem::size_of::<AtomicWord>() * 8;

pub struct BitsetWriter {
    data: Arc<BitsetData>,
    recycle_list: LinkedListWriter<RecycleNode>,
}

#[derive(Clone)]
pub struct Bitset {
    data: Arc<BitsetData>,
}

struct BitsetData {
    capacity: AcqRelUsize,
    data: RelaxedAtomicPtr<AtomicWord>,
    recycle_list: LinkedList<RecycleNode>,
}

pub struct BitsetValueIter<'a> {
    index: usize,
    current_word: u64,
    data: &'a [AtomicWord],
}

pub struct BitsetIter<'a> {
    index: usize,
    current_word: u64,
    data: &'a [AtomicWord],
}

struct RecycleNode {
    ptr: *mut AtomicWord,
    len: usize,
}

fn quot_and_rem(index: usize) -> (usize, usize) {
    (index / BITS, index % BITS)
}

impl BitsetWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        let len = (capacity + BITS - 1) / BITS;
        let capacity = len * BITS;
        let data = (0..len)
            .map(|_| AtomicWord::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let data = Box::into_raw(data) as *mut AtomicWord;
        let mut recycle_list = LinkedListWriter::new();
        recycle_list.push(RecycleNode::new(data, len));
        let data = Arc::new(BitsetData::new(data, capacity, recycle_list.list()));

        Self { data, recycle_list }
    }

    pub fn bitset(&self) -> Bitset {
        Bitset {
            data: self.data.clone(),
        }
    }

    pub fn insert(&mut self, index: usize) {
        if index >= self.capacity() {
            self.expand(index);
        }
        let (quot, rem) = quot_and_rem(index);
        let data = self.data.data();
        let slot = &data[quot];
        slot.store(slot.load() | (1 << rem));
    }

    fn expand(&mut self, index: usize) {
        let word_len = self.data.capacity.load() / BITS;
        let mut next_len = word_len;
        let add_len = std::cmp::max(word_len / 2, 1);
        loop {
            next_len += add_len;
            if next_len * BITS > index {
                break;
            }
        }

        let mut data: Vec<_> = (0..next_len).map(|_| AtomicWord::new(0)).collect();
        let current_data = self.data.data();
        for i in 0..word_len {
            data[i] = AtomicWord::new(current_data[i].load());
        }
        let data = data.into_boxed_slice();
        let data_ptr = Box::into_raw(data) as *mut AtomicWord;
        self.recycle_list.push(RecycleNode::new(data_ptr, next_len));

        // First store data_ptr then capacity
        self.data.data.store(data_ptr);
        self.data.capacity.store(next_len * BITS);
    }

    pub fn contains(&self, index: usize) -> bool {
        self.data.contains(index)
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
}

impl Bitset {
    pub fn contains(&self, index: usize) -> bool {
        self.data.contains(index)
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn word(&self, pos: usize) -> u64 {
        self.data.word(pos)
    }

    pub fn iter_words(&self) -> impl Iterator<Item = u64> + '_ {
        self.data.iter_words()
    }

    pub fn to_boolean_vec(&self) -> Vec<bool> {
        self.data.to_boolean_vec()
    }

    pub fn iter_values(&self) -> BitsetValueIter<'_> {
        self.data.iter_values()
    }

    pub fn iter(&self) -> BitsetIter<'_> {
        self.data.iter()
    }

    pub fn count_ones(&self) -> usize {
        self.data.count_ones()
    }
}

impl BitsetData {
    fn new(data: *mut AtomicWord, capacity: usize, recycle_list: LinkedList<RecycleNode>) -> Self {
        Self {
            capacity: AcqRelUsize::new(capacity),
            data: RelaxedAtomicPtr::new(data),
            recycle_list,
        }
    }

    fn capacity(&self) -> usize {
        self.capacity.load()
    }

    pub fn is_empty(&self) -> bool {
        self.capacity() == 0
    }

    fn word(&self, pos: usize) -> u64 {
        self.data().get(pos).map(|w| w.load()).unwrap_or_default()
    }

    fn data(&self) -> &[AtomicWord] {
        // First load capacity then data_ptr
        let capacity = self.capacity();
        let data_ptr = self.data.load();
        unsafe { std::slice::from_raw_parts(data_ptr, capacity / BITS) }
    }

    fn iter_words(&self) -> impl Iterator<Item = u64> + '_ {
        self.data().iter().map(|w| w.load())
    }

    fn to_boolean_vec(&self) -> Vec<bool> {
        let mut vec = Vec::with_capacity(self.capacity());
        for w in self.iter_words() {
            for i in 0..BITS {
                vec.push((w >> i) & 1 == 1);
            }
        }
        vec
    }

    fn iter_values(&self) -> BitsetValueIter<'_> {
        BitsetValueIter {
            index: 0,
            current_word: 0,
            data: self.data(),
        }
    }

    fn iter(&self) -> BitsetIter<'_> {
        BitsetIter {
            index: 0,
            current_word: 0,
            data: self.data(),
        }
    }

    fn contains(&self, index: usize) -> bool {
        let (quot, rem) = quot_and_rem(index);
        let data = self.data();
        if quot < data.len() {
            let slot = &data[quot];
            (slot.load() & (1 << rem)) != 0
        } else {
            false
        }
    }

    fn count_ones(&self) -> usize {
        self.data()
            .iter()
            .map(|w| w.load().count_ones() as usize)
            .sum()
    }
}

impl Drop for BitsetData {
    fn drop(&mut self) {
        for recycle_node in self.recycle_list.iter() {
            recycle_node.release();
        }
    }
}

impl<'a> Iterator for BitsetValueIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data.len() * BITS {
            let index = self.index;
            self.index += 1;
            if index % BITS == 0 {
                self.current_word = self.data[index / BITS].load();
            }
            Some((self.current_word & (1 << (index % BITS))) != 0)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.data.len() * BITS - self.index;
        (remain, Some(remain))
    }
}

impl<'a> Iterator for BitsetIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.data.len() * BITS {
            let index = self.index;
            self.index += 1;
            if index % BITS == 0 {
                self.current_word = self.data[index / BITS].load();
            }
            if (self.current_word & (1 << (index % BITS))) != 0 {
                return Some(index);
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.data.len() * BITS - self.index;
        (remain, Some(remain))
    }
}

impl RecycleNode {
    fn new(ptr: *mut AtomicWord, len: usize) -> Self {
        Self { ptr, len }
    }

    fn release(&self) {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.ptr, self.len));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::BITS;
    use crate::util::bitset::BitsetWriter;

    #[test]
    fn test_bits() {
        assert_eq!(BITS, 64);
    }

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
    fn test_to_boolean_vec() {
        let capacity = 127;
        let mut writer = BitsetWriter::with_capacity(capacity);
        writer.insert(0);
        writer.insert(1);
        writer.insert(127);
        let bitset = writer.bitset();
        let mut expect = vec![false; 128];
        expect[0] = true;
        expect[1] = true;
        expect[127] = true;
        let got = bitset.to_boolean_vec();
        assert_eq!(got, expect);
    }

    #[test]
    fn test_iter_values() {
        let capacity = 127;
        let mut writer = BitsetWriter::with_capacity(capacity);
        writer.insert(0);
        writer.insert(1);
        writer.insert(127);
        let bitset = writer.bitset();
        let mut expect = vec![false; 128];
        expect[0] = true;
        expect[1] = true;
        expect[127] = true;
        let got: Vec<_> = bitset.iter_values().collect();
        assert_eq!(got, expect);
    }

    #[test]
    fn test_iter() {
        let capacity = 127;
        let mut writer = BitsetWriter::with_capacity(capacity);
        writer.insert(0);
        writer.insert(1);
        writer.insert(127);
        let bitset = writer.bitset();
        let expect = vec![0, 1, 127];
        let got: Vec<_> = bitset.iter().collect();
        assert_eq!(got, expect);
    }

    #[test]
    fn test_expand() {
        let capacity = 1;
        let mut writer = BitsetWriter::with_capacity(capacity);
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
        let mut writer = BitsetWriter::with_capacity(capacity);
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
