use allocator_api2::alloc::{Allocator, Global};

use super::radix_tree::{RadixTree, RadixTreeIter, RadixTreeWriter};

pub struct ChunkedVecWriter<T, A: Allocator = Global> {
    tree: RadixTreeWriter<T, A>,
}

#[derive(Clone)]
pub struct ChunkedVec<T, A: Allocator = Global> {
    tree: RadixTree<T, A>,
}

pub struct ChunkedVecIter<'a, T, A: Allocator = Global> {
    iter: RadixTreeIter<'a, T, A>,
}

impl<T, A: Allocator + Default> ChunkedVecWriter<T, A> {
    pub fn new(chunk_exponent: usize, tree_exponent: u8) -> Self {
        Self::new_in(chunk_exponent, tree_exponent, Default::default())
    }
}

impl<T, A: Allocator> ChunkedVecWriter<T, A> {
    pub fn new_in(chunk_exponent: usize, tree_exponent: u8, allocator: A) -> Self {
        Self {
            tree: RadixTreeWriter::new_in(chunk_exponent, tree_exponent, allocator),
        }
    }

    pub fn reader(&self) -> ChunkedVec<T, A> {
        ChunkedVec {
            tree: self.tree.reader(),
        }
    }

    pub fn push(&mut self, value: T) {
        self.tree.push(value);
    }
}

impl<T, A: Allocator> ChunkedVec<T, A> {
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.tree.get(index)
    }

    pub fn iter(&self) -> ChunkedVecIter<'_, T, A> {
        ChunkedVecIter {
            iter: self.tree.iter(),
        }
    }
}

impl<'a, T, A: Allocator> Iterator for ChunkedVecIter<'a, T, A> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::util::chunked_vec::ChunkedVecWriter;

    #[test]
    fn test_basic() {
        let mut writer: ChunkedVecWriter<_> = ChunkedVecWriter::new(3, 2);
        let vec = writer.reader();
        assert_eq!(vec.len(), 0);
        let count = 1024;
        for i in 0..count {
            writer.push((i + 1) * 10);
        }

        assert_eq!(vec.len(), count);
        for i in 0..count {
            assert_eq!(vec.get(i).unwrap().clone(), (i + 1) * 10);
        }
        assert!(vec.get(count).is_none());
        for (i, &v) in vec.iter().enumerate() {
            assert_eq!((i + 1) * 10, v);
        }
    }

    #[test]
    fn test_multithreads() {
        let mut writer = ChunkedVecWriter::<usize>::new(3, 2);
        let vec = writer.reader();
        assert_eq!(vec.len(), 0);
        let count = 1024;

        let reader_thread = thread::spawn(move || loop {
            let len = vec.len();
            for i in 0..len {
                assert_eq!(vec.get(i).cloned(), Some(i * 10));
            }
            for (i, &v) in vec.iter().enumerate() {
                assert_eq!(i * 10, v);
            }
            if len == count {
                assert!(vec.get(count).is_none());
                break;
            }
            thread::yield_now();
        });

        let writer_thread = thread::spawn(move || {
            for i in 0..count {
                writer.push(i * 10);
            }
        });

        reader_thread.join().unwrap();
        writer_thread.join().unwrap();
    }
}
