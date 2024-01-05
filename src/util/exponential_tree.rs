use std::{
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering},
};

use super::{AcqRelUsize, FixedCapacityVec};

pub struct ExponentialTree<T> {
    root: AtomicPtr<ExponentialTreeNode<T>>,
    size: AcqRelUsize,
}

struct ExponentialTreeNode<T> {
    height: usize,
    exponent: usize,
    data: ExponentialTreeNodeData<T>,
}

enum ExponentialTreeNodeData<T> {
    LeafNode(FixedCapacityVec<T>),
    InternalNode(FixedCapacityVec<NonNull<ExponentialTreeNode<T>>>),
}

impl<T> ExponentialTree<T> {
    pub fn new(exponent: usize) -> Self {
        let root = Box::new(ExponentialTreeNode::new(0, exponent));

        Self {
            root: AtomicPtr::new(Box::into_raw(root)),
            size: AcqRelUsize::new(0),
        }
    }

    pub fn insert(&self, value: T) {
        let index = self.size();
        let root = self.root_growup_if_needed(index);
        root.insert(index, value);
        self.size.store(index + 1);
    }

    pub fn search(&self, index: usize) -> Option<&T> {
        if index < self.size() {
            let root = unsafe { self.root().as_ref() };
            Some(root.search(index))
        } else {
            None
        }
    }

    pub fn size(&self) -> usize {
        self.size.load()
    }

    fn root(&self) -> NonNull<ExponentialTreeNode<T>> {
        unsafe { NonNull::new_unchecked(self.root.load(Ordering::Acquire)) }
    }

    fn root_growup_if_needed(&self, index: usize) -> &ExponentialTreeNode<T> {
        let root = self.root();
        let root_ref = unsafe { root.as_ref() };
        if index < (1 << (root_ref.exponent * (root_ref.height + 1))) {
            root_ref
        } else {
            let next_root = Box::new(ExponentialTreeNode::new(
                root_ref.height + 1,
                root_ref.exponent,
            ));
            next_root.add_child(0, root);
            let next_root_ptr = unsafe { NonNull::new_unchecked(Box::into_raw(next_root)) };
            self.root.store(next_root_ptr.as_ptr(), Ordering::Release);
            unsafe { next_root_ptr.as_ref() }
        }
    }
}

impl<T> Drop for ExponentialTree<T> {
    fn drop(&mut self) {
        let _ = unsafe { Box::from_raw(self.root().as_ptr()) };
    }
}

impl<T> ExponentialTreeNode<T> {
    fn new(height: usize, exponent: usize) -> Self {
        let data = if height == 0 {
            ExponentialTreeNodeData::new_leaf(exponent)
        } else {
            ExponentialTreeNodeData::new_internal(exponent)
        };

        Self {
            height,
            exponent,
            data,
        }
    }

    fn insert(&self, index: usize, value: T) {
        let mut node = self;
        let mut index = index;
        while node.height > 0 {
            let slot_index = node.slot_index(index);
            index = node.sub_index(index);
            node = node.child_create_if_needed(slot_index);
        }
        node.add_value(index, value);
    }

    fn search(&self, index: usize) -> &T {
        let mut node = self;
        let mut index = index;
        while node.height > 0 {
            let slot = node.slot_index(index);
            index = node.sub_index(index);
            node = node.child(slot);
        }
        node.value(index)
    }

    fn slot_index(&self, index: usize) -> usize {
        debug_assert!(self.height > 0);
        index >> (self.exponent * self.height)
    }

    fn sub_index(&self, index: usize) -> usize {
        debug_assert!(self.height > 0);
        index & ((1 << (self.exponent * self.height)) - 1)
    }

    fn child(&self, index: usize) -> &ExponentialTreeNode<T> {
        match &self.data {
            ExponentialTreeNodeData::InternalNode(v) => unsafe { v[index].as_ref() },
            ExponentialTreeNodeData::LeafNode(_) => {
                panic!("ExponentialTreeNode::LeafNode get child");
            }
        }
    }

    fn child_create_if_needed(&self, index: usize) -> &ExponentialTreeNode<T> {
        match &self.data {
            ExponentialTreeNodeData::InternalNode(v) => {
                if index < v.len() {
                    unsafe { v[index].as_ref() }
                } else {
                    debug_assert_eq!(index, v.len());
                    let child = Box::new(ExponentialTreeNode::new(self.height - 1, self.exponent));
                    v.push(unsafe { NonNull::new_unchecked(Box::into_raw(child)) });
                    unsafe { v[index].as_ref() }
                }
            }
            ExponentialTreeNodeData::LeafNode(_) => {
                panic!("ExponentialTreeNode::LeafNode get child");
            }
        }
    }

    fn add_child(&self, index: usize, child: NonNull<ExponentialTreeNode<T>>) {
        match &self.data {
            ExponentialTreeNodeData::InternalNode(v) => {
                debug_assert_eq!(index, v.len());
                v.push(child);
            }
            ExponentialTreeNodeData::LeafNode(_) => {
                panic!("ExponentialTreeNode:LeafNode add_child");
            }
        }
    }

    fn value(&self, index: usize) -> &T {
        match &self.data {
            ExponentialTreeNodeData::LeafNode(v) => &v[index],
            ExponentialTreeNodeData::InternalNode(_) => {
                panic!("ExponentialTreeNode::InternalNode get value");
            }
        }
    }

    fn add_value(&self, index: usize, value: T) {
        match &self.data {
            ExponentialTreeNodeData::LeafNode(v) => {
                debug_assert_eq!(index, v.len());
                v.push(value);
            }
            ExponentialTreeNodeData::InternalNode(_) => {
                panic!("ExponentialTreeNode::InternalNode add_value");
            }
        }
    }
}

impl<T> Drop for ExponentialTreeNode<T> {
    fn drop(&mut self) {
        match &self.data {
            ExponentialTreeNodeData::InternalNode(v) => {
                for c in v.iter() {
                    let _ = unsafe { Box::from_raw(c.as_ptr()) };
                }
            }
            _ => {}
        }
    }
}

impl<T> ExponentialTreeNodeData<T> {
    fn new_leaf(exponent: usize) -> Self {
        Self::LeafNode(FixedCapacityVec::with_capacity(1 << exponent))
    }

    fn new_internal(exponent: usize) -> Self {
        Self::InternalNode(FixedCapacityVec::with_capacity(1 << exponent))
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::ExponentialTree;

    #[test]
    fn test_simple() {
        let tree = ExponentialTree::new(2);
        assert_eq!(tree.size(), 0);
        let count = 1024;
        for i in 0..count {
            tree.insert(i * 10);
        }
        for i in 0..count {
            assert_eq!(tree.search(i).unwrap().clone(), i * 10);
        }
        assert!(tree.search(count).is_none());
    }

    #[test]
    fn test_multithreads() {
        let tree = ExponentialTree::<usize>::new(2);
        assert_eq!(tree.size(), 0);
        let count = 1024;
        thread::scope(|scope| {
            let t = scope.spawn(|| loop {
                let size = tree.size();
                for i in 0..size {
                    assert_eq!(tree.search(i).unwrap().clone(), i * 10);
                }
                if size == count {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            });

            for i in 0..count {
                tree.insert(i * 10);
            }
            for i in 0..count {
                assert_eq!(tree.search(i).unwrap().clone(), i * 10);
            }
            assert!(tree.search(count).is_none());

            t.join().unwrap();
        });
    }
}
