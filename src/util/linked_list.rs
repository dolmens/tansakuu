use std::{ptr, sync::Arc};

use super::atomic::AcqRelAtomicPtr;

pub struct LinkedListWriter<T> {
    list: Arc<LinkedListData<T>>,
    tail: *mut Node<T>,
}

#[derive(Clone)]
pub struct LinkedList<T> {
    list: Arc<LinkedListData<T>>,
}

pub struct LinkedListIter<'a, T> {
    cursor: Option<&'a Node<T>>,
}

struct LinkedListData<T> {
    head: AcqRelAtomicPtr<Node<T>>,
}

struct Node<T> {
    value: T,
    next: AcqRelAtomicPtr<Node<T>>,
}

impl<T> LinkedListWriter<T> {
    pub fn new() -> Self {
        Self {
            list: Arc::new(LinkedListData::new()),
            tail: ptr::null_mut(),
        }
    }

    pub fn push(&mut self, value: T) {
        let node = Box::into_raw(Box::new(Node {
            value,
            next: AcqRelAtomicPtr::default(),
        }));

        if self.tail.is_null() {
            self.list.head.store(node);
        } else {
            let tail = unsafe { &*self.tail };
            tail.next.store(node);
        }
        self.tail = node;
    }

    pub fn list(&self) -> LinkedList<T> {
        LinkedList {
            list: self.list.clone(),
        }
    }
}

impl<T> LinkedList<T> {
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn iter(&self) -> LinkedListIter<T> {
        self.list.iter()
    }
}

impl<T> LinkedListData<T> {
    fn new() -> Self {
        Self {
            head: AcqRelAtomicPtr::default(),
        }
    }

    fn is_empty(&self) -> bool {
        self.head.load().is_null()
    }

    fn iter(&self) -> LinkedListIter<T> {
        LinkedListIter::new(self)
    }
}

impl<T> Drop for LinkedListData<T> {
    fn drop(&mut self) {
        let mut cursor = self.head.load();
        while !cursor.is_null() {
            let node = unsafe { Box::from_raw(cursor) };
            cursor = node.next.load();
        }
    }
}

impl<'a, T> LinkedListIter<'a, T> {
    fn new(list: &'a LinkedListData<T>) -> Self {
        let head = list.head.load();
        let cursor = if head.is_null() {
            None
        } else {
            Some(unsafe { &*head })
        };

        Self { cursor }
    }
}

impl<'a, T> Iterator for LinkedListIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.map(|cursor| {
            let next = cursor.next.load();
            self.cursor = if next.is_null() {
                None
            } else {
                Some(unsafe { &*next })
            };
            &cursor.value
        })
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::LinkedListWriter;

    #[test]
    fn test_basic() {
        let mut writer = LinkedListWriter::new();
        let list = writer.list();
        assert!(list.is_empty());
        let data = vec![10, 20, 30];
        for &item in data.iter() {
            writer.push(item);
        }
        assert!(!list.is_empty());
        let got: Vec<_> = list.iter().copied().collect();
        assert_eq!(data, got);
    }

    #[test]
    fn test_multithreads() {
        let mut writer = LinkedListWriter::new();
        let list = writer.list();
        let data = vec![10, 20, 30];
        let expect = data.clone();
        let reader = thread::spawn(move || {
            let got: Vec<_> = list.iter().copied().collect();
            if got != expect {
                thread::yield_now();
            }
        });

        for &item in data.iter() {
            writer.push(item);
        }

        reader.join().unwrap();
    }
}
