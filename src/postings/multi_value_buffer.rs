use std::{
    alloc::Layout,
    mem,
    ptr::{self, NonNull},
};

use allocator_api2::alloc::{Allocator, Global};

use crate::{
    postings::{Decode, Encode},
    util::AcqRelUsize,
};

use super::{ByteSliceReader, ByteSliceWriter};

#[derive(Clone)]
pub struct ValueItem {
    pub size: usize,
    pub align: usize,
    encode: Encode,
    decode: Decode,
}

#[derive(Clone)]
pub struct MultiValue {
    value_items: Vec<ValueItem>,
}

pub struct MultiValueBuffer {
    len: AcqRelUsize,
    buffer: NonNull<u8>,
    layout: Layout,
    offsets: Vec<usize>,
    value_items: MultiValue,
    capacity: usize,
    allocator: Global,
}

impl ValueItem {
    pub fn new<T>(encode: Encode, decode: Decode) -> Self {
        Self {
            size: mem::size_of::<T>(),
            align: mem::align_of::<T>(),
            encode,
            decode,
        }
    }

    pub fn encode(&self, src: &[u8], dst: &ByteSliceWriter) -> usize {
        (&self.encode)(src, dst)
    }

    pub fn decode(&self, src: &mut ByteSliceReader, dst: &mut [u8]) -> usize {
        (&self.decode)(src, dst)
    }
}

impl MultiValue {
    pub fn new() -> Self {
        Self {
            value_items: vec![],
        }
    }

    pub fn new_with_value_items(value_items: Vec<ValueItem>) -> Self {
        Self { value_items }
    }

    pub fn add_value<T>(&mut self, encode: Encode, decode: Decode) {
        self.add_value_item(ValueItem::new::<T>(encode, decode));
    }

    pub fn add_value_item(&mut self, value_item: ValueItem) {
        self.value_items.push(value_item);
    }

    pub fn value_items_count(&self) -> usize {
        self.value_items.len()
    }

    pub fn value_items(&self) -> &[ValueItem] {
        &self.value_items
    }
}

fn buffer_layout(multi_value: &MultiValue, len: usize) -> (Layout, Vec<usize>) {
    let mut layout = Layout::from_size_align(0, 1).unwrap();
    let mut offsets = vec![];
    for value_item in multi_value.value_items() {
        let (layout_next, offset) = layout
            .extend(Layout::from_size_align(value_item.size * len, value_item.align).unwrap())
            .unwrap();
        layout = layout_next;
        offsets.push(offset);
    }
    layout = layout.pad_to_align();
    (layout, offsets)
}

fn buffer_copy(
    multi_value: &MultiValue,
    len: usize,
    src: NonNull<u8>,
    src_offsets: &[usize],
    dst: NonNull<u8>,
    dst_offsets: &[usize],
) {
    for ((src_offset, dst_offset), value_item) in src_offsets
        .iter()
        .copied()
        .zip(dst_offsets.iter().copied())
        .zip(multi_value.value_items())
    {
        unsafe {
            ptr::copy_nonoverlapping(
                src.as_ptr().add(src_offset),
                dst.as_ptr().add(dst_offset),
                value_item.size * len,
            );
        }
    }
}

impl MultiValueBuffer {
    pub fn new(value_items: MultiValue, capacity: usize) -> Self {
        let allocator = Global;
        let (layout, offsets) = buffer_layout(&value_items, capacity);
        let buffer = allocator.allocate(layout).unwrap().cast::<u8>();
        Self {
            len: AcqRelUsize::new(0),
            buffer,
            layout,
            offsets,
            value_items,
            capacity,
            allocator,
        }
    }

    pub fn push<T>(&self, row: usize, value: T) {
        assert!(
            std::mem::size_of::<T>() == self.value_items().value_items()[row].size
                && std::mem::align_of::<T>() == self.value_items().value_items()[row].align
        );
        let buffer_of_row = self.row::<T>(row);
        unsafe {
            ptr::write(buffer_of_row.as_ptr().add(self.len()), value);
        }
    }

    pub fn end_push(&self) {
        self.inc_len();
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.capacity
    }

    pub fn clear(&self) {
        self.set_len(0);
    }

    pub fn snapshot(&self) -> MultiValueBuffer {
        let allocator = Global;
        let len = self.len();
        let (layout, offsets) = buffer_layout(&self.value_items, len);
        let buffer = allocator.allocate(layout).unwrap().cast::<u8>();
        buffer_copy(
            &self.value_items,
            len,
            self.buffer,
            &self.offsets,
            buffer,
            &offsets,
        );

        MultiValueBuffer {
            len: AcqRelUsize::new(len),
            buffer,
            layout,
            offsets,
            value_items: self.value_items.clone(),
            capacity: len,
            allocator,
        }
    }

    pub fn value_items(&self) -> &MultiValue {
        &self.value_items
    }

    pub fn row_data(&self, row: usize) -> NonNull<u8> {
        let offest = self.offsets[row];
        unsafe { NonNull::new_unchecked(self.buffer.as_ptr().add(offest).cast::<u8>()) }
    }

    pub fn row<T>(&self, row: usize) -> NonNull<T> {
        assert_eq!(
            std::mem::size_of::<T>(),
            self.value_items.value_items()[row].size
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            self.value_items.value_items()[row].align
        );
        self.row_data(row).cast::<T>()
    }

    pub fn row_slice<T>(&self, row: usize) -> &[T] {
        let row_data = self.row::<T>(row);
        unsafe { &*std::ptr::slice_from_raw_parts(row_data.as_ptr(), self.len()) }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.len.load()
    }

    pub fn inc_len(&self) {
        self.len.store(self.len() + 1);
    }

    pub fn set_len(&self, len: usize) {
        self.len.store(len);
    }
}

impl Drop for MultiValueBuffer {
    fn drop(&mut self) {
        unsafe {
            self.allocator.deallocate(self.buffer, self.layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::postings::{copy_decode, copy_encode};

    use super::{MultiValue, MultiValueBuffer};

    #[test]
    fn test_simple() {
        let mut value_items = MultiValue::new();
        value_items.add_value::<i8>(copy_encode, copy_decode);
        value_items.add_value::<i64>(copy_encode, copy_decode);
        value_items.add_value::<i32>(copy_encode, copy_decode);

        let capacity = 4;
        let mbuffer = MultiValueBuffer::new(value_items, capacity);

        assert_eq!(mbuffer.capacity(), capacity);
        assert_eq!(mbuffer.len(), 0);
        assert!(!mbuffer.is_full());

        mbuffer.push::<i8>(0, 1);
        mbuffer.push::<i64>(1, 1001);
        mbuffer.push::<i32>(2, 2001);
        assert_eq!(mbuffer.len(), 0);
        mbuffer.end_push();
        assert_eq!(mbuffer.len(), 1);
        assert!(!mbuffer.is_full());
        assert_eq!(mbuffer.row_slice::<i8>(0), &[1]);
        assert_eq!(mbuffer.row_slice::<i64>(1), &[1001]);
        assert_eq!(mbuffer.row_slice::<i32>(2), &[2001]);

        mbuffer.push::<i8>(0, 2);
        mbuffer.push::<i64>(1, 1002);
        mbuffer.push::<i32>(2, 2002);
        mbuffer.end_push();
        assert_eq!(mbuffer.len(), 2);
        assert!(!mbuffer.is_full());
        assert_eq!(mbuffer.row_slice::<i8>(0), &[1, 2]);
        assert_eq!(mbuffer.row_slice::<i64>(1), &[1001, 1002]);
        assert_eq!(mbuffer.row_slice::<i32>(2), &[2001, 2002]);

        mbuffer.push::<i8>(0, 3);
        mbuffer.push::<i64>(1, 1003);
        mbuffer.push::<i32>(2, 2003);
        mbuffer.end_push();
        assert_eq!(mbuffer.len(), 3);
        assert!(!mbuffer.is_full());
        assert_eq!(mbuffer.row_slice::<i8>(0), &[1, 2, 3]);
        assert_eq!(mbuffer.row_slice::<i64>(1), &[1001, 1002, 1003]);
        assert_eq!(mbuffer.row_slice::<i32>(2), &[2001, 2002, 2003]);

        let snapshot = mbuffer.snapshot();
        assert_eq!(snapshot.capacity(), 3);
        assert_eq!(snapshot.len(), 3);
        assert!(snapshot.is_full());
        assert_eq!(snapshot.row_slice::<i8>(0), &[1, 2, 3]);
        assert_eq!(snapshot.row_slice::<i64>(1), &[1001, 1002, 1003]);
        assert_eq!(snapshot.row_slice::<i32>(2), &[2001, 2002, 2003]);

        mbuffer.push::<i8>(0, 4);
        mbuffer.push::<i64>(1, 1004);
        mbuffer.push::<i32>(2, 2004);
        mbuffer.end_push();
        assert_eq!(mbuffer.len(), 4);
        assert!(mbuffer.is_full());
        assert_eq!(mbuffer.row_slice::<i8>(0), &[1, 2, 3, 4]);
        assert_eq!(mbuffer.row_slice::<i64>(1), &[1001, 1002, 1003, 1004]);
        assert_eq!(mbuffer.row_slice::<i32>(2), &[2001, 2002, 2003, 2004]);

        // snapshot remain unchanged
        assert_eq!(snapshot.capacity(), 3);
        assert_eq!(snapshot.len(), 3);
        assert!(snapshot.is_full());
        assert_eq!(snapshot.row_slice::<i8>(0), &[1, 2, 3]);
        assert_eq!(snapshot.row_slice::<i64>(1), &[1001, 1002, 1003]);
        assert_eq!(snapshot.row_slice::<i32>(2), &[2001, 2002, 2003]);

        let snapshot = mbuffer.snapshot();
        assert_eq!(snapshot.capacity(), 4);
        assert_eq!(snapshot.len(), 4);
        assert!(snapshot.is_full());
        assert_eq!(snapshot.row_slice::<i8>(0), &[1, 2, 3, 4]);
        assert_eq!(snapshot.row_slice::<i64>(1), &[1001, 1002, 1003, 1004]);
        assert_eq!(snapshot.row_slice::<i32>(2), &[2001, 2002, 2003, 2004]);

        mbuffer.clear();
        assert_eq!(mbuffer.len(), 0);
        assert!(!mbuffer.is_full());

        mbuffer.push::<i8>(0, 4);
        mbuffer.push::<i64>(1, 5);
        mbuffer.push::<i32>(2, 6);
        mbuffer.end_push();
        assert_eq!(mbuffer.len(), 1);
        assert_eq!(mbuffer.row_slice::<i8>(0), &[4]);
        assert_eq!(mbuffer.row_slice::<i64>(1), &[5]);
        assert_eq!(mbuffer.row_slice::<i32>(2), &[6]);
    }
}
