use std::{
    borrow::Borrow,
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    ptr::NonNull,
    sync::Arc,
};

use super::{
    atomic::{AcqRelAtomicPtr, AcqRelUsize},
    capacity_policy::{CapacityPolicy, FixedCapacityPolicy},
    fixed_size_bitset::{FixedSizeBitset, FixedSizeBitsetWriter},
    raw::Raw,
};

const SPARSITY: usize = 2;

pub struct LayeredHashMapWriter<
    K,
    V,
    S: BuildHasher = RandomState,
    C: CapacityPolicy = FixedCapacityPolicy,
> {
    item_count: usize,
    capacity: usize,
    initial_capacity: usize,
    capacity_policy: C,
    head: Option<LayerWriter<K, V>>,
    data: Arc<LayeredHashMapData<K, V, S>>,
}

unsafe impl<K: Send + Sync, V: Send + Sync, S: BuildHasher + Send, C: CapacityPolicy + Send> Send
    for LayeredHashMapWriter<K, V, S, C>
{
}

#[derive(Clone)]
pub struct LayeredHashMap<K, V, S: BuildHasher = RandomState> {
    data: Arc<LayeredHashMapData<K, V, S>>,
}

struct LayeredHashMapData<K, V, S: BuildHasher> {
    head: AcqRelAtomicPtr<Layer<K, V>>,
    item_count: AcqRelUsize,
    capacity: AcqRelUsize,
    hasher_builder: S,
}

struct LayerWriter<K, V> {
    capacity: usize,
    bitset: FixedSizeBitsetWriter,
    layer: NonNull<Layer<K, V>>,
}

struct Layer<K, V> {
    next: Option<NonNull<Layer<K, V>>>,
    bitset: FixedSizeBitset,
    elements: Box<[Raw<Element<K, V>>]>,
}

struct Element<K, V> {
    key: K,
    value: V,
}

pub struct Iter<'a, K, V> {
    inner_iter: Option<LayerInnerIter<'a, K, V>>,
    next_layer: Option<NonNull<Layer<K, V>>>,
}

struct LayerInnerIter<'a, K, V> {
    cursor: usize,
    bitset: &'a FixedSizeBitset,
    elements: &'a [Raw<Element<K, V>>],
}

fn make_hash<Q: ?Sized + Hash, S: BuildHasher>(key: &Q, hasher_builder: &S) -> u64 {
    let mut hasher = hasher_builder.build_hasher();
    key.hash(&mut hasher);
    hasher.finish()
}

impl<K, V, S: BuildHasher, C: CapacityPolicy> LayeredHashMapWriter<K, V, S, C> {
    pub fn with_capacity(initial_capacity: usize, hasher_builder: S, capacity_policy: C) -> Self {
        let data = Arc::new(LayeredHashMapData::new(hasher_builder));

        Self {
            item_count: 0,
            capacity: 0,
            initial_capacity,
            capacity_policy,
            head: None,
            data,
        }
    }

    pub fn hashmap(&self) -> LayeredHashMap<K, V, S> {
        LayeredHashMap {
            data: self.data.clone(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.item_count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.data.contains_key(key)
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.data.get(key)
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        self.data.iter()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V>
    where
        K: Eq + Hash,
    {
        if self.head.is_none() || self.item_count * SPARSITY > self.capacity {
            let mut head = self.allocate_layer();
            head.layer_mut().next = self.head.as_ref().map(|layer| layer.layer);
            self.data.head.store(head.layer.as_ptr());
            self.capacity += head.capacity();
            self.data.capacity.store(self.capacity);
            self.head = Some(head);
        }

        let hash = make_hash(&key, &self.data.hasher_builder);
        let head_ref = self.head.as_mut().unwrap();
        match head_ref.insert(hash, key, value) {
            Some(v) => Some(v),
            None => {
                self.item_count += 1;
                self.data.item_count.store(self.item_count);
                None
            }
        }
    }

    fn allocate_layer(&self) -> LayerWriter<K, V> {
        let curent_capacity = if self.capacity > 0 {
            self.capacity
        } else {
            self.initial_capacity
        };
        let capacity = self.capacity_policy.next_capacity(curent_capacity);
        LayerWriter::with_capacity(capacity)
    }
}

impl<K, V, S: BuildHasher> LayeredHashMap<K, V, S> {
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.data.contains_key(key)
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.data.get(key)
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        self.data.iter()
    }
}

impl<K, V, S: BuildHasher> LayeredHashMapData<K, V, S> {
    fn new(hasher_builder: S) -> Self {
        Self {
            head: AcqRelAtomicPtr::new(std::ptr::null_mut()),
            item_count: AcqRelUsize::new(0),
            capacity: AcqRelUsize::new(0),
            hasher_builder,
        }
    }

    fn iter(&self) -> Iter<'_, K, V> {
        let head = NonNull::new(self.head.load());
        let (inner_iter, next_layer) = match head {
            Some(head) => {
                let head_ref = unsafe { head.as_ref() };
                (Some(head_ref.iter()), head_ref.next)
            }
            None => (None, None),
        };

        Iter {
            inner_iter,
            next_layer,
        }
    }

    fn capacity(&self) -> usize {
        self.capacity.load()
    }

    fn len(&self) -> usize {
        self.item_count.load()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = make_hash(key, &self.hasher_builder);
        let mut layer_ptr = self.head.load();
        if layer_ptr.is_null() {
            return false;
        }
        loop {
            let layer = unsafe { &*layer_ptr };
            if layer.contains_key(hash, key) {
                return true;
            }
            layer_ptr = match layer.next {
                Some(next) => next.as_ptr(),
                None => break,
            }
        }
        false
    }

    fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = make_hash(key, &self.hasher_builder);
        let mut layer_ptr = self.head.load();
        if layer_ptr.is_null() {
            return None;
        }
        loop {
            let layer = unsafe { &*layer_ptr };
            if let Some(value) = layer.get(hash, key) {
                return Some(value);
            }
            layer_ptr = match layer.next {
                Some(ptr) => ptr.as_ptr(),
                None => break,
            };
        }
        None
    }
}

impl<K, V, S: BuildHasher> Drop for LayeredHashMapData<K, V, S> {
    fn drop(&mut self) {
        let mut layer_ptr = self.head.load();
        if !layer_ptr.is_null() {
            loop {
                let layer = unsafe { Box::from_raw(layer_ptr) };
                layer_ptr = match layer.next {
                    Some(next) => next.as_ptr(),
                    None => break,
                }
            }
        }
    }
}

impl<K, V> LayerWriter<K, V> {
    fn with_capacity(capacity: usize) -> Self {
        let bitset = FixedSizeBitsetWriter::with_capacity(capacity);
        let layer = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Layer::with_capacity(
                capacity,
                bitset.bitset(),
            ))))
        };

        Self {
            capacity,
            bitset,
            layer,
        }
    }

    fn layer(&self) -> &Layer<K, V> {
        unsafe { self.layer.as_ref() }
    }

    fn layer_mut(&mut self) -> &mut Layer<K, V> {
        unsafe { self.layer.as_mut() }
    }

    fn insert(&mut self, hash: u64, key: K, value: V) -> Option<V>
    where
        K: Eq + Hash,
    {
        let layer = self.layer();
        let mut index = (hash as usize) % self.capacity();
        while self.bitset.contains(index) {
            let bucket = layer.bucket(index);
            if bucket.key == key {
                return Some(value);
            }
            index += 1;
            if index >= self.capacity() {
                index = 0;
            }
        }

        unsafe {
            layer.elements[index].write(Element::new(key, value));
        }

        self.bitset.insert(index);

        None
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<K, V> Layer<K, V> {
    fn with_capacity(capacity: usize, bitset: FixedSizeBitset) -> Self {
        let elements: Vec<_> = (0..capacity).map(|_| Raw::new()).collect();
        Self {
            next: None,
            bitset,
            elements: elements.into_boxed_slice(),
        }
    }

    fn iter(&self) -> LayerInnerIter<'_, K, V> {
        LayerInnerIter {
            cursor: 0,
            bitset: &self.bitset,
            elements: &self.elements,
        }
    }

    fn contains_key<Q: ?Sized>(&self, hash: u64, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let mut index = (hash as usize) % self.capacity();
        while self.bitset.contains(index) {
            let bucket = self.bucket(index);
            if bucket.key.borrow() == key {
                return true;
            }
            index += 1;
            if index >= self.capacity() {
                index = 0;
            }
        }
        false
    }

    fn get<Q: ?Sized>(&self, hash: u64, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let mut index = (hash as usize) % self.capacity();
        while self.bitset.contains(index) {
            let bucket = self.bucket(index);
            if bucket.key.borrow() == key {
                return Some(&bucket.value);
            }
            index += 1;
            if index >= self.capacity() {
                index = 0;
            }
        }
        None
    }

    fn bucket(&self, index: usize) -> &Element<K, V> {
        unsafe { self.elements[index].get() }
    }

    fn capacity(&self) -> usize {
        self.elements.len()
    }
}

impl<K, V> Drop for Layer<K, V> {
    fn drop(&mut self) {
        for i in 0..self.elements.len() {
            if self.bitset.contains(i) {
                unsafe {
                    self.elements[i].drop();
                }
            }
        }
    }
}

impl<K, V> Element<K, V> {
    fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(inner_iter) = self.inner_iter.as_mut() {
                if let Some(kv) = inner_iter.next() {
                    return Some(kv);
                }
                match self.next_layer {
                    Some(next) => {
                        let next_ref = unsafe { next.as_ref() };
                        self.inner_iter = Some(next_ref.iter());
                        self.next_layer = next_ref.next;
                    }
                    None => break,
                };
            } else {
                break;
            }
        }
        None
    }
}

impl<'a, K, V> Iterator for LayerInnerIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < self.elements.len() {
            if self.bitset.contains(self.cursor) {
                let bucket = unsafe { self.elements[self.cursor].get() };
                self.cursor += 1;
                return Some((&bucket.key, &bucket.value));
            }
            self.cursor += 1;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::hash_map::RandomState, thread};

    use crate::util::{
        capacity_policy::FixedCapacityPolicy,
        layered_hashmap::{make_hash, LayerWriter, LayeredHashMapWriter},
    };

    #[test]
    fn test_layer_simple() {
        let mut layer = LayerWriter::with_capacity(4);
        let hasher_builder = RandomState::new();

        let key = 1;
        let hash = make_hash(&key, &hasher_builder);
        assert!(layer.layer().get(hash, &key).is_none());
        assert!(layer.insert(hash, key, 10).is_none());

        assert_eq!(layer.insert(hash, key, 20).unwrap(), 20);
        assert_eq!(layer.layer().get(hash, &key).unwrap().clone(), 10);

        let key2 = 2;
        let hash2 = make_hash(&key2, &hasher_builder);
        assert!(layer.insert(hash2, key2, 20).is_none());

        let _ = unsafe { Box::from_raw(layer.layer.as_ptr()) };
    }

    #[test]
    fn test_hashmap_simple() {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let mut writer = LayeredHashMapWriter::with_capacity(4, hasher_builder, capacity_policy);
        let map = writer.hashmap();
        assert!(map.get(&1).is_none());
        assert!(writer.insert(1, 10).is_none());
        assert_eq!(map.get(&1).unwrap().clone(), 10);

        assert!(map.get(&2).is_none());
        assert!(writer.insert(2, 20).is_none());
        assert_eq!(map.get(&2).unwrap().clone(), 20);
        assert_eq!(map.get(&1).unwrap().clone(), 10);

        assert!(map.get(&3).is_none());
        assert!(writer.insert(3, 30).is_none());
        assert_eq!(map.get(&3).unwrap().clone(), 30);
        assert_eq!(map.get(&2).unwrap().clone(), 20);
        assert_eq!(map.get(&1).unwrap().clone(), 10);
        assert!(map.get(&0).is_none());

        assert!(map.get(&4).is_none());
        assert!(writer.insert(4, 40).is_none());
        assert_eq!(map.get(&4).unwrap().clone(), 40);
        assert_eq!(map.get(&3).unwrap().clone(), 30);
        assert_eq!(map.get(&2).unwrap().clone(), 20);
        assert_eq!(map.get(&1).unwrap().clone(), 10);
        assert!(map.get(&0).is_none());

        assert!(map.get(&5).is_none());
        assert!(writer.insert(5, 50).is_none());
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
        let mut writer =
            LayeredHashMapWriter::<i32, i32>::with_capacity(4, hasher_builder, capacity_policy);
        let map = writer.hashmap();
        let count = 16;
        let reader_thread = thread::spawn(move || {
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
                        thread::yield_now();
                    }
                } else {
                    assert!(map.get(&i).is_none());
                }
            }
        });

        let writer_thread = thread::spawn(move || {
            for i in (0..count).rev() {
                if i % 2 == 0 {
                    writer.insert(i, i * 10);
                }
            }
        });

        reader_thread.join().unwrap();
        writer_thread.join().unwrap();
    }

    #[test]
    fn test_layer_inner_iter() {
        let mut layer = LayerWriter::with_capacity(8);
        let hasher_builder = RandomState::new();

        let key1 = 1;
        let hash1 = make_hash(&key1, &hasher_builder);
        assert!(layer.insert(hash1, key1, 10).is_none());
        let key2 = 3;
        let hash2 = make_hash(&key2, &hasher_builder);
        assert!(layer.insert(hash2, key2, 30).is_none());
        let key3 = 5;
        let hash3 = make_hash(&key3, &hasher_builder);
        assert!(layer.insert(hash3, key3, 50).is_none());

        let mut items: Vec<_> = layer.layer().iter().map(|(&k, &v)| (k, v)).collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(items, vec![(1, 10), (3, 30), (5, 50)]);

        let _ = unsafe { Box::from_raw(layer.layer.as_ptr()) };
    }

    #[test]
    fn test_iter() {
        let hasher_builder = RandomState::new();
        let capacity_policy = FixedCapacityPolicy;
        let mut writer =
            LayeredHashMapWriter::<i32, i32>::with_capacity(4, hasher_builder, capacity_policy);
        let map = writer.hashmap();
        let mut expected = vec![];
        for i in 0..8 {
            expected.push((i, i * 10));
            assert!(writer.insert(i, i * 10).is_none());
        }
        let mut items: Vec<_> = map.iter().map(|(&k, &v)| (k, v)).collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(items, expected);
    }
}
