use super::radix_tree::{RadixTree, RadixTreeIntoIter, RadixTreeIter, RadixTreeWriter};

pub struct ChunkedVecWriter<T> {
    tree: RadixTreeWriter<T>,
}

#[derive(Clone)]
pub struct ChunkedVec<T> {
    tree: RadixTree<T>,
}

pub struct ChunkedVecIter<'a, T> {
    iter: RadixTreeIter<'a, T>,
}

pub struct ChunkedVecIntoIter<T> {
    iter: RadixTreeIntoIter<T>,
}

impl<T> ChunkedVecWriter<T> {
    pub fn new(chunk_size: usize, node_size: usize) -> Self {
        Self {
            tree: RadixTreeWriter::new(chunk_size, node_size),
        }
    }

    pub fn reader(&self) -> ChunkedVec<T> {
        ChunkedVec {
            tree: self.tree.reader(),
        }
    }

    pub fn push(&mut self, value: T) {
        self.tree.push(value);
    }
}

impl<T> ChunkedVec<T> {
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.tree.get(index)
    }

    pub fn iter(&self) -> ChunkedVecIter<'_, T> {
        ChunkedVecIter {
            iter: self.tree.iter(),
        }
    }
}

impl<T> IntoIterator for ChunkedVec<T>
where
    T: Clone,
{
    type Item = T;
    type IntoIter = ChunkedVecIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        ChunkedVecIntoIter {
            iter: self.tree.into_iter(),
        }
    }
}

impl<'a, T> Iterator for ChunkedVecIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<T> Iterator for ChunkedVecIntoIter<T>
where
    T: Clone,
{
    type Item = T;

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
        let mut writer: ChunkedVecWriter<_> = ChunkedVecWriter::new(8, 4);
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
        let mut writer = ChunkedVecWriter::<usize>::new(8, 4);
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
