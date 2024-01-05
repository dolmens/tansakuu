use super::{AcqRelUsize, ExponentialTree, FixedCapacityVec};

pub struct ChunkedVector<T> {
    len: AcqRelUsize,
    chunk_exponent: usize,
    chunk_tree: ExponentialTree<FixedCapacityVec<T>>,
}

impl<T> ChunkedVector<T> {
    pub fn new(chunk_exponent: usize, tree_exponent: usize) -> Self {
        Self {
            len: AcqRelUsize::new(0),
            chunk_exponent,
            chunk_tree: ExponentialTree::new(tree_exponent),
        }
    }

    pub fn push(&self, value: T) {
        let len = self.len();
        if len % (1 << self.chunk_exponent) == 0 {
            self.chunk_tree
                .insert(FixedCapacityVec::with_capacity(1 << self.chunk_exponent));
        }
        let chunk = self
            .chunk_tree
            .search(len / (1 << self.chunk_exponent))
            .unwrap();
        chunk.push(value);
        self.len.store(len + 1);
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        let len = self.len();
        if index < len {
            let chunk = self
                .chunk_tree
                .search(index / (1 << self.chunk_exponent))
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

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::ChunkedVector;

    #[test]
    fn test_simple() {
        let vec = ChunkedVector::new(3, 2);
        assert_eq!(vec.len(), 0);
        let count = 1024;
        for i in 0..count {
            vec.push((i + 1) * 10);
            assert_eq!(vec.len(), i + 1);
        }

        for i in 0..count {
            assert_eq!(vec.get(i).unwrap().clone(), (i + 1) * 10);
        }
    }

    #[test]
    fn test_multithreads() {
        let vec = ChunkedVector::<usize>::new(3, 2);
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
                vec.push((i + 1) * 10);
                assert_eq!(vec.len(), i + 1);
            }

            t.join().unwrap();
        });
    }
}
