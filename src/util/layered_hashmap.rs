use std::{
    borrow::Borrow,
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use super::{Bitset, CapacityPolicy, FixedCapacityPolicy, Raw};

pub struct LayeredHashMap<
    K,
    V,
    S: BuildHasher = RandomState,
    C: CapacityPolicy = FixedCapacityPolicy,
> {
    head: AtomicPtr<HashBucket<K, V>>,
    hasher_builder: S,
    capacity_policy: C,
}

pub struct Iter<'a, K, V> {
    drained: bool,
    head: NonNull<HashBucket<K, V>>,
    iter: BucketIter<'a, K, V>,
}

struct Element<K, V> {
    key: K,
    value: V,
}

impl<K, V> Element<K, V> {
    fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

struct HashBucket<K, V> {
    next: Option<NonNull<HashBucket<K, V>>>,
    bitset: Bitset,
    item_count: AtomicUsize,
    elements: Box<[Raw<Element<K, V>>]>,
}

struct BucketIter<'a, K, V> {
    cursor: usize,
    bitset: &'a Bitset,
    elements: &'a [Raw<Element<K, V>>],
}

unsafe impl<K: Send + Sync, V: Send + Sync> Send for HashBucket<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for HashBucket<K, V> {}

fn make_hash<Q: ?Sized + Hash, S: BuildHasher>(key: &Q, hasher_builder: &S) -> u64 {
    let mut hasher = hasher_builder.build_hasher();
    key.hash(&mut hasher);
    hasher.finish()
}

impl<K, V, S: BuildHasher, C: CapacityPolicy> LayeredHashMap<K, V, S, C> {
    pub fn with_capacity(initial_capacity: usize, hasher_builder: S, capacity_policy: C) -> Self {
        let bucket = Self::allocate_bucket(initial_capacity);
        Self {
            head: AtomicPtr::new(Box::into_raw(bucket)),
            hasher_builder,
            capacity_policy,
        }
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        let head = self.head();
        let iter = unsafe { head.as_ref().iter() };

        Iter {
            drained: false,
            head,
            iter,
        }
    }

    pub unsafe fn insert(&self, key: K, value: V) -> Option<V>
    where
        K: Eq + Hash,
    {
        let head = self.head_or_add_bucket_if_saturated();
        head.insert(key, value, &self.hasher_builder)
    }

    pub fn is_empty(&self) -> bool {
        let head = self.head_ref();
        head.is_empty() && head.next.is_none()
    }

    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let mut head_ptr = self.head();
        loop {
            let head = unsafe { head_ptr.as_ref() };
            if head.contains_key(key, &self.hasher_builder) {
                return true;
            }
            match head.next {
                Some(next) => head_ptr = next,
                None => break,
            }
        }
        false
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let mut head_ptr = self.head();
        loop {
            let head = unsafe { head_ptr.as_ref() };
            if let Some(value) = head.get(key, &self.hasher_builder) {
                return Some(value);
            }
            match head.next {
                Some(next) => head_ptr = next,
                None => break,
            }
        }
        None
    }

    fn head(&self) -> NonNull<HashBucket<K, V>> {
        unsafe { NonNull::new_unchecked(self.head.load(Ordering::Acquire)) }
    }

    fn head_ref(&self) -> &HashBucket<K, V> {
        unsafe { self.head().as_ref() }
    }

    fn head_or_add_bucket_if_saturated(&self) -> &HashBucket<K, V> {
        let head_ptr = self.head();
        let head = unsafe { head_ptr.as_ref() };
        if head.is_saturated() {
            let capacity = self.capacity_policy.next_capacity(head.capacity());
            let mut bucket = Self::allocate_bucket(capacity);
            bucket.next = Some(head_ptr);
            let bucket_ptr = unsafe { NonNull::new_unchecked(Box::into_raw(bucket)) };
            self.head.store(bucket_ptr.as_ptr(), Ordering::Release);
            unsafe { bucket_ptr.as_ref() }
        } else {
            head
        }
    }

    fn allocate_bucket(capacity: usize) -> Box<HashBucket<K, V>> {
        Box::new(HashBucket::with_capacity(capacity))
    }
}

impl<K, V, S: BuildHasher, C: CapacityPolicy> Drop for LayeredHashMap<K, V, S, C> {
    fn drop(&mut self) {
        let mut head_ptr = self.head();
        loop {
            unsafe {
                let head = Box::from_raw(head_ptr.as_ptr());
                match head.next {
                    Some(next) => head_ptr = next,
                    None => break,
                }
            }
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.drained {
            if let Some(kv) = self.iter.next() {
                return Some(kv);
            }
            unsafe {
                if let Some(next) = self.head.as_ref().next {
                    self.head = next;
                    self.iter = self.head.as_ref().iter();
                } else {
                    self.drained = true;
                }
            }
        }

        None
    }
}

impl<K, V> HashBucket<K, V> {
    fn with_capacity(capacity: usize) -> Self {
        let elements: Vec<_> = (0..capacity).map(|_| Raw::new()).collect();
        Self {
            next: None,
            bitset: Bitset::with_capacity(capacity),
            item_count: AtomicUsize::new(0),
            elements: elements.into_boxed_slice(),
        }
    }

    fn iter(&self) -> BucketIter<'_, K, V> {
        BucketIter {
            cursor: 0,
            bitset: &self.bitset,
            elements: &self.elements,
        }
    }

    fn insert<S: BuildHasher>(&self, key: K, value: V, hasher_builder: &S) -> Option<V>
    where
        K: Eq + Hash,
    {
        let hash = make_hash(&key, hasher_builder) as usize;
        let mut index = hash % self.capacity();
        while self.bitset.contains(index) {
            let entry = self.entry(index);
            if entry.key == key {
                return Some(value);
            }
            index += 1;
            if index >= self.capacity() {
                index = 0;
            }
        }
        self.write_entry(index, key, value);
        self.inc_item_count();
        unsafe {
            self.bitset.insert(index);
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.bitset.is_empty()
    }

    fn contains_key<Q: ?Sized, S: BuildHasher>(&self, key: &Q, hasher_builder: &S) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = make_hash(key, hasher_builder) as usize;
        let mut index = hash % self.capacity();
        while self.bitset.contains(index) {
            let entry = self.entry(index);
            if entry.key.borrow() == key {
                return true;
            }
            index += 1;
            if index >= self.capacity() {
                index = 0;
            }
        }
        false
    }

    fn get<Q: ?Sized, S: BuildHasher>(&self, key: &Q, hasher_builder: &S) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = make_hash(key, hasher_builder) as usize;
        let mut index = hash % self.capacity();
        while self.bitset.contains(index) {
            let entry = self.entry(index);
            if entry.key.borrow() == key {
                return Some(&entry.value);
            }
            index += 1;
            if index >= self.capacity() {
                index = 0;
            }
        }
        None
    }

    fn write_entry(&self, index: usize, key: K, value: V) {
        unsafe {
            self.elements[index].write(Element::new(key, value));
        }
    }

    fn entry(&self, index: usize) -> &Element<K, V> {
        unsafe { self.elements[index].get() }
    }

    fn capacity(&self) -> usize {
        self.elements.len()
    }

    fn is_saturated(&self) -> bool {
        self.capacity() <= self.item_count() * 2
    }

    fn item_count(&self) -> usize {
        self.item_count.load(Ordering::Relaxed)
    }

    fn inc_item_count(&self) {
        self.item_count
            .store(self.item_count() + 1, Ordering::Relaxed);
    }
}

impl<'a, K, V> Iterator for BucketIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < self.elements.len() {
            if self.bitset.contains(self.cursor) {
                let entry = unsafe { self.elements[self.cursor].get() };
                self.cursor += 1;
                return Some((&entry.key, &entry.value));
            }
            self.cursor += 1;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::hash_map::RandomState, thread, time::Duration};

    use super::{FixedCapacityPolicy, HashBucket, LayeredHashMap};

    #[test]
    fn test_hashbucket_simple() {
        let bucket = HashBucket::with_capacity(4);
        let hasher_builder = RandomState::new();

        assert!(bucket.get(&1, &hasher_builder).is_none());
        assert!(bucket.insert(1, 10, &hasher_builder).is_none());
        assert_eq!(bucket.insert(1, 20, &hasher_builder).unwrap(), 20);
        assert_eq!(bucket.get(&1, &hasher_builder).unwrap().clone(), 10);

        assert!(bucket.insert(2, 20, &hasher_builder).is_none());
    }

    #[test]
    fn test_hashmap_simple() {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let map = LayeredHashMap::with_capacity(4, hasher_builder, capacity_policy);
        assert!(map.get(&1).is_none());
        unsafe {
            assert!(map.insert(1, 10).is_none());
        }
        assert_eq!(map.get(&1).unwrap().clone(), 10);

        assert!(map.get(&2).is_none());
        unsafe {
            assert!(map.insert(2, 20).is_none());
        }
        assert_eq!(map.get(&2).unwrap().clone(), 20);
        assert_eq!(map.get(&1).unwrap().clone(), 10);

        assert!(map.get(&3).is_none());
        unsafe {
            assert!(map.insert(3, 30).is_none());
        }
        assert_eq!(map.get(&3).unwrap().clone(), 30);
        assert_eq!(map.get(&2).unwrap().clone(), 20);
        assert_eq!(map.get(&1).unwrap().clone(), 10);
        assert!(map.get(&0).is_none());

        assert!(map.get(&4).is_none());
        unsafe {
            assert!(map.insert(4, 40).is_none());
        }
        assert_eq!(map.get(&4).unwrap().clone(), 40);
        assert_eq!(map.get(&3).unwrap().clone(), 30);
        assert_eq!(map.get(&2).unwrap().clone(), 20);
        assert_eq!(map.get(&1).unwrap().clone(), 10);
        assert!(map.get(&0).is_none());

        assert!(map.get(&5).is_none());
        unsafe {
            assert!(map.insert(5, 50).is_none());
        }
        assert_eq!(map.get(&5).unwrap().clone(), 50);
        assert_eq!(map.get(&4).unwrap().clone(), 40);
        assert_eq!(map.get(&3).unwrap().clone(), 30);
        assert_eq!(map.get(&2).unwrap().clone(), 20);
        assert_eq!(map.get(&1).unwrap().clone(), 10);
        assert!(map.get(&0).is_none());
    }

    #[test]
    fn test_hashmap_multithreads() {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let map = LayeredHashMap::<i32, i32>::with_capacity(4, hasher_builder, capacity_policy);
        let count = 16;
        thread::scope(|scope| {
            let t = scope.spawn(|| {
                // Node that the keys were inserted in reverse order.
                // If a key is found, then the keys after it must also exist.
                for i in 0..count {
                    if i % 2 == 0 {
                        if let Some(&v) = map.get(&i) {
                            assert_eq!(v, i * 10);
                            for j in i..count {
                                if j % 2 == 0 {
                                    assert_eq!(map.get(&j).unwrap().clone(), j * 10);
                                }
                            }
                            if i == 0 {
                                break;
                            }
                        } else {
                            thread::sleep(Duration::from_millis(1));
                        }
                    } else {
                        assert!(map.get(&i).is_none());
                    }
                }
            });

            for i in (0..count).rev() {
                if i % 2 == 0 {
                    unsafe {
                        map.insert(i, i * 10);
                    }
                }
            }

            t.join().unwrap();
        });
    }

    #[test]
    fn test_bucket_iter() {
        let bucket = HashBucket::with_capacity(8);
        let hasher_builder = RandomState::new();

        assert!(bucket.insert(1, 10, &hasher_builder).is_none());
        assert!(bucket.insert(3, 30, &hasher_builder).is_none());
        assert!(bucket.insert(5, 50, &hasher_builder).is_none());

        let mut items: Vec<_> = bucket.iter().map(|(&k, &v)| (k, v)).collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(items, vec![(1, 10), (3, 30), (5, 50)])
    }

    #[test]
    fn test_iter() {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let map = LayeredHashMap::<i32, i32>::with_capacity(4, hasher_builder, capacity_policy);
        let mut expected = vec![];
        for i in 0..8 {
            expected.push((i, i * 10));
            unsafe {
                assert!(map.insert(i, i * 10).is_none());
            }
        }
        let mut items: Vec<_> = map.iter().map(|(&k, &v)| (k, v)).collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(items, expected);
    }
}
