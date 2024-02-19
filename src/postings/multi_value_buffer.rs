use std::{
    alloc::{handle_alloc_error, Layout},
    cell::Cell,
    collections::HashMap,
    ptr::{self, NonNull},
    sync::Arc,
};

use crate::{
    arena::BumpArena,
    util::atomic::{AcqRelUsize, RelaxedAtomicPtr, RelaxedU32, RelaxedU8},
};

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum ValueItem {
    U32,
    U8,
}

#[derive(Clone)]
pub struct MultiValue {
    value_items: Box<[ValueItem]>,
    item_layouts: Box<[Layout]>,
}

pub struct MultiValueBuilder {
    value_items: Vec<ValueItem>,
}

#[derive(Clone)]
pub struct MultiValueBufferPool {
    inner: Arc<MultiValueBufferPoolInner>,
}

struct MultiValueBufferPoolInner {
    pool: Cell<HashMap<Box<[ValueItem]>, HashMap<usize, Vec<NonNull<u8>>>>>,
    arena: BumpArena,
}

pub struct MultiValueBuffer {
    capacity: AcqRelUsize,
    data: RelaxedAtomicPtr<u8>,
    value_items: MultiValue,
}

impl ValueItem {
    pub fn layout(&self) -> Layout {
        match self {
            Self::U32 => Layout::new::<RelaxedU32>(),
            Self::U8 => Layout::new::<RelaxedU8>(),
        }
    }
}

impl MultiValueBuilder {
    pub fn new() -> Self {
        Self {
            value_items: vec![],
        }
    }

    pub fn add_value_item(mut self, value_item: ValueItem) -> Self {
        self.value_items.push(value_item);
        Self {
            value_items: self.value_items,
        }
    }

    pub fn build(self) -> MultiValue {
        MultiValue::new(self.value_items.into_boxed_slice())
    }
}

impl MultiValue {
    pub fn new(value_items: Box<[ValueItem]>) -> Self {
        let item_layouts: Vec<_> = value_items.iter().map(|v| v.layout()).collect();

        Self {
            value_items,
            item_layouts: item_layouts.into_boxed_slice(),
        }
    }

    pub fn item_count(&self) -> usize {
        self.value_items.len()
    }

    pub fn value_item(&self, index: usize) -> ValueItem {
        self.value_items[index]
    }

    pub fn item_layout(&self, index: usize) -> Layout {
        self.item_layouts[index]
    }

    pub fn value_items(&self) -> &[ValueItem] {
        &self.value_items
    }

    pub fn item_layouts(&self) -> &[Layout] {
        &self.item_layouts
    }

    pub fn buffer_layout(&self, capacity: usize) -> (Layout, Box<[usize]>) {
        let mut layout = Layout::new::<usize>();
        let mut offsets = vec![];
        for item_layout in self.item_layouts() {
            let (layout_next, offset) = layout
                .extend(
                    Layout::from_size_align(item_layout.size() * capacity, item_layout.align())
                        .unwrap(),
                )
                .unwrap();
            layout = layout_next;
            offsets.push(offset);
        }

        (layout.pad_to_align(), offsets.into_boxed_slice())
    }
}

fn multi_value_buffer_copy(
    value_items: &MultiValue,
    src_buffer: NonNull<u8>,
    src_capacity: usize,
    dst_buffer: NonNull<u8>,
    dst_capacity: usize,
) {
    debug_assert!(src_capacity <= dst_capacity);
    let (_, src_offsets) = value_items.buffer_layout(src_capacity);
    let (_, dst_offsets) = value_items.buffer_layout(dst_capacity);
    for (&value_item, (&src_offset, &dst_offset)) in value_items
        .value_items()
        .iter()
        .zip(src_offsets.iter().zip(dst_offsets.iter()))
    {
        match value_item {
            ValueItem::U32 => {
                let src_data = unsafe { src_buffer.as_ptr().add(src_offset) } as *mut RelaxedU32;
                let dst_data = unsafe { dst_buffer.as_ptr().add(dst_offset) } as *mut RelaxedU32;
                for i in 0..src_capacity {
                    let src = unsafe { &*src_data.add(i) };
                    let dst = unsafe { &*dst_data.add(i) };
                    dst.store(src.load());
                }
            }
            ValueItem::U8 => {
                let src_data = unsafe { src_buffer.as_ptr().add(src_offset) } as *mut RelaxedU8;
                let dst_data = unsafe { dst_buffer.as_ptr().add(dst_offset) } as *mut RelaxedU8;
                for i in 0..src_capacity {
                    let src = unsafe { &*src_data.add(i) };
                    let dst = unsafe { &*dst_data.add(i) };
                    dst.store(src.load());
                }
            }
        }
    }
}

impl MultiValueBuffer {
    pub fn new(value_items: MultiValue) -> Self {
        Self {
            capacity: AcqRelUsize::new(0),
            data: RelaxedAtomicPtr::default(),
            value_items,
        }
    }

    pub fn expand(&self, capacity: usize, buffer_pool: &MultiValueBufferPool) {
        let current_capacity = self.capacity.load();
        let buffer = buffer_pool.allocate(self.multi_value(), capacity);
        if current_capacity > 0 {
            let current_buffer = self.data().unwrap();
            multi_value_buffer_copy(
                self.multi_value(),
                current_buffer,
                current_capacity,
                buffer,
                capacity,
            );
            self.data.store(buffer.as_ptr());
            self.capacity.store(capacity);
            buffer_pool.release(self.multi_value(), current_capacity, current_buffer);
        } else {
            self.data.store(buffer.as_ptr());
            self.capacity.store(capacity);
        }
    }

    pub fn buffer_layout(&self, capacity: usize) -> (Layout, Box<[usize]>) {
        self.value_items.buffer_layout(capacity)
    }

    pub fn data(&self) -> Option<NonNull<u8>> {
        let data = self.data.load();
        NonNull::new(data)
    }

    pub fn capacity(&self) -> usize {
        self.capacity.load()
    }

    pub fn multi_value(&self) -> &MultiValue {
        &self.value_items
    }
}

impl MultiValueBufferPool {
    pub fn new(arena: BumpArena) -> Self {
        Self {
            inner: Arc::new(MultiValueBufferPoolInner::new(arena)),
        }
    }

    pub fn allocate(&self, multi_value: &MultiValue, capacity: usize) -> NonNull<u8> {
        let pool = unsafe { &mut *self.inner.pool.as_ptr() };
        if let Some(buffer) = pool
            .get_mut(multi_value.value_items())
            .and_then(|m| m.get_mut(&capacity))
            .and_then(|v| v.pop())
        {
            return buffer;
        }

        let (layout, offsets) = multi_value.buffer_layout(capacity);
        let buffer = self
            .inner
            .arena
            .allocate(layout)
            .unwrap_or_else(|_| handle_alloc_error(layout))
            .cast::<u8>();

        unsafe {
            ptr::write(buffer.as_ptr() as *mut usize, capacity);
            for (&value_item, &offset) in multi_value.value_items().iter().zip(offsets.iter()) {
                let ptr = buffer.as_ptr().add(offset);
                match value_item {
                    ValueItem::U32 => {
                        let data = ptr as *mut RelaxedU32;
                        for j in 0..capacity {
                            ptr::write(data.add(j), RelaxedU32::new(0))
                        }
                    }
                    ValueItem::U8 => {
                        let data = ptr as *mut RelaxedU8;
                        for j in 0..capacity {
                            ptr::write(data.add(j), RelaxedU8::new(0))
                        }
                    }
                }
            }
        }

        buffer
    }

    pub fn release(&self, multi_value: &MultiValue, capacity: usize, data: NonNull<u8>) {
        let pool = unsafe { &mut *self.inner.pool.as_ptr() };
        pool.entry(multi_value.value_items().into())
            .or_insert_with(|| HashMap::new())
            .entry(capacity)
            .or_insert_with(|| vec![])
            .push(data);
    }

    pub fn len(&self) -> usize {
        let pool = unsafe { &*self.inner.pool.as_ptr() };
        pool.len()
    }
}

impl MultiValueBufferPoolInner {
    pub fn new(arena: BumpArena) -> Self {
        Self {
            pool: Cell::new(HashMap::new()),
            arena,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;

    use crate::{
        arena::BumpArena,
        postings::{MultiValueBuilder, ValueItem},
        util::atomic::{RelaxedU32, RelaxedU8},
    };

    use super::{MultiValueBuffer, MultiValueBufferPool};

    #[test]
    fn test_multi_value() {
        let multi_value = MultiValueBuilder::new()
            .add_value_item(ValueItem::U8)
            .add_value_item(ValueItem::U32)
            .add_value_item(ValueItem::U8)
            .build();
        assert_eq!(multi_value.item_count(), 3);
        let layout_u8 = Layout::new::<RelaxedU8>();
        let layout_u32 = Layout::new::<RelaxedU32>();
        assert_eq!(multi_value.item_layout(0), layout_u8);
        assert_eq!(multi_value.item_layout(1), layout_u32);
        assert_eq!(multi_value.item_layout(2), layout_u8);
        let (layout, offsets) = multi_value.buffer_layout(3);
        assert_eq!(layout.size(), 32);
        assert_eq!(layout.align(), 8); // <- usize header align
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets[0], 8); // <- usize header
        assert_eq!(offsets[1], 12);
        assert_eq!(offsets[2], 24);
    }
    #[test]
    fn test_buffer_pool() {
        let multi_value1 = MultiValueBuilder::new()
            .add_value_item(ValueItem::U8)
            .add_value_item(ValueItem::U32)
            .add_value_item(ValueItem::U8)
            .build();
        let multi_value1_1 = MultiValueBuilder::new()
            .add_value_item(ValueItem::U8)
            .add_value_item(ValueItem::U32)
            .add_value_item(ValueItem::U8)
            .build();
        let multi_value2 = MultiValueBuilder::new()
            .add_value_item(ValueItem::U8)
            .add_value_item(ValueItem::U32)
            .build();

        let arena = BumpArena::new();
        let pool = MultiValueBufferPool::new(arena.clone());

        let buf1 = pool.allocate(&multi_value1, 2);
        let buf2 = pool.allocate(&multi_value1, 2);
        pool.release(&multi_value1, 2, buf1);
        pool.release(&multi_value1, 2, buf2);
        let buf3 = pool.allocate(&multi_value1_1, 2);
        assert_eq!(buf2, buf3);
        pool.release(&multi_value1, 2, buf3);
        let buf4 = pool.allocate(&multi_value1, 2);
        assert_eq!(buf2, buf4);
        let buf5 = pool.allocate(&multi_value1, 2);
        assert_eq!(buf1, buf5);
        pool.release(&multi_value1, 2, buf5);
        let buf6 = pool.allocate(&multi_value2, 2);
        assert_ne!(buf1, buf6);
        let buf7 = pool.allocate(&multi_value1, 4);
        assert_ne!(buf1, buf7);
        let buf8 = pool.allocate(&multi_value1, 2);
        assert_eq!(buf1, buf8);
    }

    #[test]
    fn test_multi_value_buffer() {
        let multi_value = MultiValueBuilder::new()
            .add_value_item(ValueItem::U8)
            .add_value_item(ValueItem::U32)
            .add_value_item(ValueItem::U8)
            .build();
        let multi_value_buffer = MultiValueBuffer::new(multi_value);
        assert_eq!(multi_value_buffer.capacity(), 0);
        assert_eq!(multi_value_buffer.data(), None);

        let arena = BumpArena::new();
        let pool = MultiValueBufferPool::new(arena.clone());

        let capacity = 2;
        multi_value_buffer.expand(capacity, &pool);
        assert_eq!(pool.len(), 0);
        let (_, offsets) = multi_value_buffer.buffer_layout(capacity);
        assert_eq!(multi_value_buffer.capacity(), capacity);
        let data = multi_value_buffer.data().unwrap();
        let first_buffer = data;
        unsafe {
            let data0 = data.as_ptr().add(offsets[0]) as *mut RelaxedU8;
            (&*data0.add(0)).store(1);
            (&*data0.add(1)).store(2);

            let data1 = data.as_ptr().add(offsets[1]) as *mut RelaxedU32;
            (&*data1.add(0)).store(3);
            (&*data1.add(1)).store(4);

            let data2 = data.as_ptr().add(offsets[2]) as *mut RelaxedU8;
            (&*data2.add(0)).store(5);
            (&*data2.add(1)).store(6);
        }
        let capacity = 4;
        multi_value_buffer.expand(capacity, &pool);
        assert_eq!(pool.len(), 1);
        assert_eq!(multi_value_buffer.capacity(), 4);
        let data = multi_value_buffer.data().unwrap();
        assert_ne!(first_buffer, data);
        let (_, offsets) = multi_value_buffer.buffer_layout(capacity);
        unsafe {
            let data0 = data.as_ptr().add(offsets[0]) as *mut RelaxedU8;
            assert_eq!((&*data0.add(0)).load(), 1);
            assert_eq!((&*data0.add(1)).load(), 2);
            assert_eq!((&*data0.add(2)).load(), 0);
            assert_eq!((&*data0.add(3)).load(), 0);

            let data1 = data.as_ptr().add(offsets[1]) as *mut RelaxedU32;
            assert_eq!((&*data1.add(0)).load(), 3);
            assert_eq!((&*data1.add(1)).load(), 4);
            assert_eq!((&*data1.add(2)).load(), 0);
            assert_eq!((&*data1.add(3)).load(), 0);

            let data2 = data.as_ptr().add(offsets[2]) as *mut RelaxedU8;
            assert_eq!((&*data2.add(0)).load(), 5);
            assert_eq!((&*data2.add(1)).load(), 6);
            assert_eq!((&*data2.add(2)).load(), 0);
            assert_eq!((&*data2.add(3)).load(), 0);
        }

        let multi_value2 = MultiValueBuilder::new()
            .add_value_item(ValueItem::U8)
            .add_value_item(ValueItem::U32)
            .add_value_item(ValueItem::U8)
            .build();
        let multi_value_buffer2 = MultiValueBuffer::new(multi_value2);
        assert_eq!(multi_value_buffer2.capacity(), 0);
        assert_eq!(multi_value_buffer2.data(), None);

        let capacity = 2;
        multi_value_buffer2.expand(capacity, &pool);
        assert_eq!(multi_value_buffer2.capacity(), capacity);
        let buffer2 = multi_value_buffer2.data().unwrap();
        assert_eq!(first_buffer, buffer2);
    }
}
