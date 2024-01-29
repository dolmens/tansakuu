use std::{alloc::Layout, marker::PhantomData, ptr::NonNull, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use super::atomic::{AcqRelAtomicPtr, AcqRelUsize};

pub struct RadixTreeWriter<T, A: Allocator = Global> {
    element_count: usize,
    chunk_exponent: u8,
    last_chunk: NonNull<T>,
    data: Arc<RadixTreeData<T, A>>,
}

unsafe impl<T: Send + Sync, A: Allocator + Send + Sync> Send for RadixTreeWriter<T, A> {}

#[derive(Clone)]
pub struct RadixTree<T, A: Allocator = Global> {
    data: Arc<RadixTreeData<T, A>>,
}

unsafe impl<T: Send + Sync, A: Allocator + Send + Sync> Send for RadixTree<T, A> {}
unsafe impl<T: Send + Sync, A: Allocator + Send + Sync> Sync for RadixTree<T, A> {}

pub struct RadixTreeIter<'a, T, A: Allocator = Global> {
    index: usize,
    size: usize,
    mask: usize,
    chunk: NonNull<T>,
    data: &'a RadixTreeData<T, A>,
}

struct RadixTreeData<T, A: Allocator = Global> {
    element_count: AcqRelUsize,
    chunk_exponent: u8,
    node_exponent: u8,
    root: AcqRelAtomicPtr<RadixTreeNode<T>>,
    allocator: A,
}

struct RadixTreeNode<T> {
    height: u8,
    exponent: u8,
    shift: u8,
    mask: u8,
    _slots: [*mut u8; 0],
    _marker: PhantomData<T>,
}

fn calculate_exponent(num: usize) -> u8 {
    let mut expoent: u8 = 0;
    let mut value: usize = 1;
    while  value < num {
        expoent += 1;
        value = value << 1;
    }
    expoent
}

impl<T, A: Allocator + Default> RadixTreeWriter<T, A> {
    pub fn new(chunk_size: usize, node_size: usize) -> Self {
        Self::new_in(chunk_size, node_size, Default::default())
    }
}

impl<T, A: Allocator> RadixTreeWriter<T, A> {
    pub fn new_in(chunk_size: usize, node_size: usize, allocator: A) -> Self {
        let chunk_exponent = calculate_exponent(chunk_size);
        let node_exponent = calculate_exponent(node_size);
        let data = Arc::new(RadixTreeData {
            element_count: AcqRelUsize::new(0),
            chunk_exponent,
            node_exponent,
            root: AcqRelAtomicPtr::default(),
            allocator,
        });

        Self {
            element_count: 0,
            chunk_exponent,
            last_chunk: NonNull::dangling(),
            data,
        }
    }

    pub fn reader(&self) -> RadixTree<T, A> {
        RadixTree {
            data: self.data.clone(),
        }
    }

    pub fn push(&mut self, value: T) {
        let index = self.element_count;
        let chunk_index_mask = (1 << self.chunk_exponent) - 1;
        let index_in_chunk = index & chunk_index_mask;
        if index_in_chunk == 0 {
            let chunk_index = index >> self.chunk_exponent;
            self.last_chunk = self.data.allocate_chunk(chunk_index);
        }

        unsafe {
            let element_ptr = self.last_chunk.as_ptr().add(index_in_chunk);
            std::ptr::write(element_ptr, value);
        }

        self.element_count += 1;
        self.data.element_count.store(self.element_count);
    }

    pub fn len(&self) -> usize {
        // Should be equal to data.element_count
        self.element_count
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }
}

impl<T, A: Allocator> RadixTree<T, A> {
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }

    pub fn iter(&self) -> RadixTreeIter<'_, T, A> {
        self.data.iter()
    }
}

impl<T, A: Allocator> RadixTreeData<T, A> {
    fn len(&self) -> usize {
        self.element_count.load()
    }

    fn get(&self, index: usize) -> Option<&T> {
        if index < self.len() {
            let chunk_index = index >> self.chunk_exponent;
            let chunk = self.get_chunk(chunk_index);
            let chunk_index_mask = (1 << self.chunk_exponent) - 1;
            unsafe { Some(&*chunk.as_ptr().add(index & chunk_index_mask)) }
        } else {
            None
        }
    }

    fn iter(&self) -> RadixTreeIter<'_, T, A> {
        RadixTreeIter::new(self)
    }

    fn get_chunk(&self, index: usize) -> NonNull<T> {
        let mut node = self.root();
        loop {
            let node_ref = unsafe { node.as_ref() };
            let index_in_slot = self.index_in_slot(node_ref, index);
            let slot_ptr = self.slot_ptr(node);
            let slot = unsafe { *slot_ptr.as_ptr().add(index_in_slot) };
            if node_ref.height == 0 {
                return unsafe { NonNull::new_unchecked(slot) }.cast();
            }
            node = unsafe { NonNull::new_unchecked(slot) }.cast();
        }
    }

    fn index_in_slot(&self, node_ref: &RadixTreeNode<T>, index: usize) -> usize {
        (index >> node_ref.shift) & (node_ref.mask as usize)
    }

    fn slot_ptr(&self, node: NonNull<RadixTreeNode<T>>) -> NonNull<*mut u8> {
        unsafe {
            let slots = node.as_ptr().add(1) as *mut *mut u8;
            NonNull::new_unchecked(slots)
        }
    }

    fn allocate_chunk(&self, index: usize) -> NonNull<T> {
        let layout = self.chunk_layout();
        let chunk = self.allocator.allocate(layout).unwrap().cast();

        self.append_chunk(chunk, index);

        chunk
    }

    fn append_chunk(&self, chunk: NonNull<T>, index: usize) {
        let root = self.root_create_if_needed();
        let root = self.root_growup_if_needed(root, index);

        let mut parent = root;
        loop {
            let parent_ref = unsafe { parent.as_ref() };
            let slot_ptr = self.slot_ptr(parent);
            let index_in_slot = self.index_in_slot(parent_ref, index);
            let slot_raw = unsafe { slot_ptr.as_ptr().add(index_in_slot) };
            let slot = unsafe { *slot_raw };
            if parent_ref.height == 0 {
                unsafe {
                    debug_assert!(slot.is_null());
                    *slot_raw = chunk.as_ptr().cast();
                }
                break;
            }
            if slot.is_null() {
                let child = self.new_node(parent_ref.height - 1);
                unsafe {
                    *slot_raw = child.as_ptr().cast();
                }
            }
            parent = unsafe { NonNull::new_unchecked((*slot_raw).cast()) };
        }
    }

    fn root(&self) -> NonNull<RadixTreeNode<T>> {
        unsafe { NonNull::new_unchecked(self.root.load()) }
    }

    fn root_create_if_needed(&self) -> NonNull<RadixTreeNode<T>> {
        let root = self.root.load();
        if !root.is_null() {
            unsafe { NonNull::new_unchecked(root) }
        } else {
            let root = self.new_node(0);
            self.root.store(root.as_ptr());
            root
        }
    }

    fn root_growup_if_needed(
        &self,
        root: NonNull<RadixTreeNode<T>>,
        index: usize,
    ) -> NonNull<RadixTreeNode<T>> {
        let root_ref = unsafe { root.as_ref() };
        if index < (1 << (root_ref.exponent * (root_ref.height + 1))) {
            root
        } else {
            let new_root = self.new_node(root_ref.height + 1);
            let new_root_slot_ptr = self.slot_ptr(new_root);
            unsafe {
                *new_root_slot_ptr.as_ptr() = root.as_ptr().cast();
            }
            self.root.store(new_root.as_ptr());
            new_root
        }
    }

    fn new_node(&self, height: u8) -> NonNull<RadixTreeNode<T>> {
        let layout = self.node_layout();
        let node_ptr = self
            .allocator
            .allocate(layout)
            .unwrap()
            .cast::<RadixTreeNode<T>>();
        let slot_ptr = self.slot_ptr(node_ptr);
        for i in 0..(1 << self.node_exponent) {
            unsafe {
                std::ptr::write(slot_ptr.as_ptr().add(i), std::ptr::null_mut());
            }
        }
        let node_ref = unsafe { &mut *node_ptr.as_ptr() };
        node_ref.height = height;
        node_ref.exponent = self.node_exponent;
        node_ref.shift = self.node_exponent * height;
        node_ref.mask = (1 << self.node_exponent) - 1;

        node_ptr
    }

    fn drop_node(&self, node: NonNull<RadixTreeNode<T>>) {
        let node_ref = unsafe { node.as_ref() };
        let layout = self.node_layout();
        let chunk_layout = self.chunk_layout();
        let slot_ptr = self.slot_ptr(node);
        for i in 0..(1 << self.node_exponent) {
            let slot = unsafe { *slot_ptr.as_ptr().add(i) };
            if slot.is_null() {
                break;
            }
            if node_ref.height == 0 {
                let chunk = unsafe { NonNull::new_unchecked(slot) }.cast();
                unsafe {
                    self.allocator.deallocate(chunk, chunk_layout);
                }
            } else {
                let child_node = unsafe { NonNull::new_unchecked(slot) }.cast();
                self.drop_node(child_node);
            }
        }
        unsafe {
            self.allocator.deallocate(node.cast(), layout);
        }
    }

    fn node_layout(&self) -> Layout {
        let node_layout = Layout::new::<RadixTreeNode<T>>();
        let slot_size = 1 << self.node_exponent;
        let data_layout = Layout::array::<*mut u8>(slot_size).unwrap();
        let layout = node_layout.extend(data_layout).unwrap().0;

        layout
    }

    fn chunk_layout(&self) -> Layout {
        let chunk_size = 1 << self.chunk_exponent;
        let layout = Layout::array::<T>(chunk_size).unwrap();

        layout
    }
}

impl<T, A: Allocator> Drop for RadixTreeData<T, A> {
    fn drop(&mut self) {
        let root = self.root.load();
        if !root.is_null() {
            let node = unsafe { NonNull::new_unchecked(root) };
            self.drop_node(node);
        }
    }
}

impl<'a, T, A: Allocator> RadixTreeIter<'a, T, A> {
    fn new(data: &'a RadixTreeData<T, A>) -> Self {
        Self {
            index: 0,
            size: data.len(),
            mask: (1 << data.chunk_exponent) - 1,
            chunk: NonNull::dangling(),
            data,
        }
    }
}

impl<'a, T, A: Allocator> Iterator for RadixTreeIter<'a, T, A> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.size {
            let index_in_chunk = self.index & self.mask;
            if index_in_chunk == 0 {
                let chunk_index = self.index >> self.data.chunk_exponent;
                self.chunk = self.data.get_chunk(chunk_index);
            }
            self.index += 1;
            unsafe { Some(&*self.chunk.as_ptr().add(index_in_chunk)) }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::util::radix_tree::RadixTreeWriter;

    #[test]
    fn test_basic() {
        let mut writer: RadixTreeWriter<_> = RadixTreeWriter::new(8, 4);
        let tree = writer.reader();
        let count = 1024;
        for i in 0..count {
            writer.push(i * 10);
        }
        for i in 0..count {
            assert_eq!(tree.get(i).unwrap().clone(), i * 10);
        }
        assert!(tree.get(count).is_none());
        for (i, &v) in tree.iter().enumerate() {
            assert_eq!(i * 10, v);
        }
    }

    #[test]
    fn test_multithreads() {
        let mut writer: RadixTreeWriter<_> = RadixTreeWriter::new(8, 4);
        let tree = writer.reader();

        let count = 1024;

        let reader_thread = thread::spawn(move || loop {
            let len = tree.len();
            for i in 0..len {
                assert_eq!(tree.get(i).cloned(), Some(i * 10));
            }
            for (i, &v) in tree.iter().enumerate() {
                assert_eq!(i * 10, v);
            }
            if len == count {
                assert!(tree.get(count).is_none());
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
