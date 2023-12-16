use std::{cell::Cell, ptr::NonNull};

use super::AcqRelAtomicPtr;

pub struct PrependLinkedList<T> {
    head: AcqRelAtomicPtr<PrependLinkedNode<T>>,
}

struct PrependLinkedNode<T> {
    value: T,
    next: Option<NonNull<PrependLinkedNode<T>>>,
}

impl<T> PrependLinkedNode<T> {
    fn new(value: T) -> Self {
        Self { value, next: None }
    }
}

impl<T> PrependLinkedList<T> {
    pub fn new() -> Self {
        Self {
            head: AcqRelAtomicPtr::default(),
        }
    }

    pub fn insert(&self, value: T) {
        let mut node = Box::new(PrependLinkedNode::new(value));
        let head_ptr = self.head.load();
        if !head_ptr.is_null() {
            node.next = Some(unsafe { NonNull::new_unchecked(head_ptr) });
        }
        self.head.store_with_box(node);
    }
}

pub struct AppendLinkedList<T> {
    head: AcqRelAtomicPtr<AppendLinkedNode<T>>,
    tail: Cell<Option<NonNull<AppendLinkedNode<T>>>>,
}

struct AppendLinkedNode<T> {
    value: T,
    next: AcqRelAtomicPtr<AppendLinkedNode<T>>,
}

impl<T> AppendLinkedNode<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            next: AcqRelAtomicPtr::default(),
        }
    }
}

impl<T> AppendLinkedList<T> {
    pub fn new() -> Self {
        Self {
            head: AcqRelAtomicPtr::default(),
            tail: Cell::new(None),
        }
    }

    pub fn insert(&self, value: T) {
        let node = Box::new(AppendLinkedNode::new(value));
        let node_ptr = Box::into_raw(node);
        if let Some(tail_ptr) = self.tail.get() {
            unsafe {
                tail_ptr.as_ref().next.store(node_ptr);
            }
        }
        self.tail
            .set(Some(unsafe { NonNull::new_unchecked(node_ptr) }));
    }

    // pub fn head(&self) -> Option<NonNull<>>
}

impl<T> Drop for AppendLinkedList<T> {
    fn drop(&mut self) {}
}
