use super::{AcqRelUsize, ExponentialTree, FixedCapacityVec};

pub struct ChunkedVec<T> {
    len: AcqRelUsize,
    chunk_exponent: usize,
    chunk_tree: ExponentialTree<FixedCapacityVec<T>>,
}

pub struct Iter<'a, T> {
    cursor: usize,
    len: usize,
    chunked_vec: &'a ChunkedVec<T>,
}

impl<T> ChunkedVec<T> {
    pub fn new(chunk_exponent: usize, tree_exponent: usize) -> Self {
        Self {
            len: AcqRelUsize::new(0),
            chunk_exponent,
            chunk_tree: ExponentialTree::new(tree_exponent),
        }
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter::new(self)
    }

    pub unsafe fn push(&self, value: T) {
        let len = self.len();
        if len % (1 << self.chunk_exponent) == 0 {
            unsafe {
                self.chunk_tree
                    .insert(FixedCapacityVec::with_capacity(1 << self.chunk_exponent));
            }
        }
        let chunk = self
            .chunk_tree
            .get(len / (1 << self.chunk_exponent))
            .unwrap();
        unsafe {
            chunk.push(value);
        }
        self.len.store(len + 1);
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        let len = self.len();
        if index < len {
            let chunk = self
                .chunk_tree
                .get(index / (1 << self.chunk_exponent))
                .unwrap();
            chunk.get(index % (1 << self.chunk_exponent))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.len.load()
    }
}

impl<'a, T> Iter<'a, T> {
    pub fn new(chunked_vec: &'a ChunkedVec<T>) -> Self {
        Self {
            cursor: 0,
            len: chunked_vec.len(),
            chunked_vec,
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.len {
            let cursor = self.cursor;
            self.cursor += 1;
            self.chunked_vec.get(cursor)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::ChunkedVec;

    #[test]
    fn test_simple() {
        let vec = ChunkedVec::new(3, 2);
        assert_eq!(vec.len(), 0);
        let count = 1024;
        for i in 0..count {
            unsafe {
                vec.push((i + 1) * 10);
            }
            assert_eq!(vec.len(), i + 1);
        }

        for i in 0..count {
            assert_eq!(vec.get(i).unwrap().clone(), (i + 1) * 10);
        }
    }

    #[test]
    fn test_multithreads() {
        let vec = ChunkedVec::<usize>::new(3, 2);
        assert_eq!(vec.len(), 0);
        let count = 1024;

        thread::scope(|scope| {
            let t = scope.spawn(|| loop {
                let len = vec.len();
                for i in 0..len {
                    assert_eq!(vec.get(i).unwrap().clone(), (i + 1) * 10);
                }
                if len == count {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            });

            for i in 0..count {
                unsafe {
                    vec.push((i + 1) * 10);
                }
                assert_eq!(vec.len(), i + 1);
            }

            t.join().unwrap();
        });
    }
}
