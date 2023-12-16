use std::{
    alloc::Layout,
    cell::Cell,
    marker::PhantomData,
    mem,
    ptr::{self, NonNull},
    slice,
};

use allocator_api2::alloc::{Allocator, Global};

use crate::util::{AcqRelUsize, RelaxedAtomicPtr, RelaxedUsize};

pub struct ByteSlice {
    next: RelaxedAtomicPtr<ByteSlice>,
    capacity: usize,
    size: RelaxedUsize,
    data: NonNull<u8>,
}

pub struct ByteSliceList {
    total_size: AcqRelUsize,
    head: NonNull<ByteSlice>,
}

unsafe impl Send for ByteSliceList {}
unsafe impl Sync for ByteSliceList {}

pub struct ByteSliceWriter {
    byte_slice_list: ByteSliceList,
    tail: Cell<NonNull<ByteSlice>>,
    allocator: Global,
}

pub struct ByteSliceReader<'a> {
    total_size: usize,
    global_offset: usize,
    current_slice_offset: usize,
    current_slice: NonNull<ByteSlice>,
    _lifetime: PhantomData<&'a ()>,
}

impl ByteSlice {
    pub fn new(capacity: usize, data: NonNull<u8>) -> Self {
        Self {
            next: RelaxedAtomicPtr::new(ptr::null_mut()),
            capacity,
            size: RelaxedUsize::default(),
            data,
        }
    }

    pub fn is_full(&self) -> bool {
        self.capacity() == self.size()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn size(&self) -> usize {
        self.size.load()
    }

    pub fn inc_size(&self, inc_size: usize) {
        self.size.store(self.size() + inc_size);
    }

    pub fn next_ptr(&self) -> NonNull<ByteSlice> {
        NonNull::new(self.next.load()).unwrap()
    }

    pub fn next(&self) -> &ByteSlice {
        unsafe { self.next.load().as_ref().unwrap() }
    }

    pub fn set_next(&self, next: NonNull<ByteSlice>) {
        self.next.store(next.as_ptr());
    }

    pub fn data(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data.as_ptr(), self.size()) }
    }

    pub fn data_slice(&self, offset: usize, len: usize) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data.as_ptr().add(offset), len) }
    }

    pub fn data_mut(&self) -> &mut [u8] {
        let capacity = self.capacity();
        let size = self.size();
        unsafe { slice::from_raw_parts_mut(self.data.as_ptr().add(size), capacity - size) }
    }
}

impl ByteSliceList {
    pub fn new(head: NonNull<ByteSlice>) -> Self {
        Self {
            total_size: AcqRelUsize::new(0),
            head,
        }
    }

    pub fn total_size(&self) -> usize {
        self.total_size.load()
    }

    fn inc_total_size(&self, inc_size: usize) {
        self.total_size.store(self.total_size() + inc_size);
    }

    pub fn head(&self) -> &ByteSlice {
        unsafe { self.head.as_ref() }
    }
}

impl ByteSliceWriter {
    pub fn with_initial_capacity(initial_capacity: usize) -> Self {
        let allocator = Global;
        let head = Self::create_slice(initial_capacity, &allocator);

        Self {
            byte_slice_list: ByteSliceList::new(head),
            tail: Cell::new(head),
            allocator,
        }
    }

    pub fn byte_slice_list(&self) -> &ByteSliceList {
        &self.byte_slice_list
    }

    pub fn write<T>(&self, value: T) {
        let data = unsafe {
            &*ptr::slice_from_raw_parts(&value as *const T as *const u8, mem::size_of::<T>())
        };
        self.write_data(data);
    }

    pub fn write_data(&self, data: &[u8]) {
        let data_len = data.len();
        let mut data = data;
        while !data.is_empty() {
            let tail = self.tail();
            if tail.is_full() {
                self.add_slice();
            } else {
                let data_dst = tail.data_mut();
                let len = std::cmp::min(data_dst.len(), data.len());
                data_dst[..len].copy_from_slice(&data[..len]);
                tail.inc_size(len);
                data = &data[len..];
            }
        }

        self.inc_total_size(data_len);
    }

    fn add_slice(&self) {
        let tail = self.tail();
        let byte_slice = Self::create_slice(tail.capacity(), &self.allocator);
        tail.set_next(byte_slice);
        self.set_tail(byte_slice);
    }

    fn create_slice(capacity: usize, allocator: &Global) -> NonNull<ByteSlice> {
        let layout = Layout::new::<ByteSlice>()
            .extend(Layout::from_size_align(capacity, 1).unwrap())
            .unwrap()
            .0;
        let byte_slice_ptr = allocator.allocate(layout).unwrap().cast::<ByteSlice>();
        unsafe {
            let byte_slice_data_ptr =
                NonNull::new_unchecked(byte_slice_ptr.as_ptr().add(1).cast::<u8>());
            ptr::write(
                byte_slice_ptr.as_ptr(),
                ByteSlice::new(capacity, byte_slice_data_ptr),
            );
        }
        byte_slice_ptr
    }

    pub fn head(&self) -> &ByteSlice {
        self.byte_slice_list.head()
    }

    fn tail(&self) -> &ByteSlice {
        unsafe { self.tail.get().as_ref() }
    }

    fn set_tail(&self, tail: NonNull<ByteSlice>) {
        self.tail.set(tail);
    }

    pub fn total_size(&self) -> usize {
        self.byte_slice_list.total_size()
    }

    fn inc_total_size(&self, inc_size: usize) {
        self.byte_slice_list.inc_total_size(inc_size);
    }
}

impl Drop for ByteSliceWriter {
    fn drop(&mut self) {
        let mut current_slice = self.byte_slice_list.head;
        loop {
            let current_slice_ref = unsafe { current_slice.as_ref() };
            let next_silce = current_slice_ref.next.load();
            let capacity = current_slice_ref.capacity;
            let layout = Layout::new::<ByteSlice>()
                .extend(Layout::from_size_align(capacity, 1).unwrap())
                .unwrap()
                .0;
            unsafe {
                self.allocator.deallocate(current_slice.cast(), layout);
                if next_silce.is_null() {
                    break;
                }
                current_slice = NonNull::new_unchecked(next_silce);
            }
        }
    }
}

impl<'a> ByteSliceReader<'a> {
    pub fn open(byte_slice_list: &'a ByteSliceList) -> Self {
        Self {
            total_size: byte_slice_list.total_size(),
            global_offset: 0,
            current_slice_offset: 0,
            current_slice: byte_slice_list.head,
            _lifetime: PhantomData,
        }
    }

    pub fn eof(&self) -> bool {
        self.tell() == self.total_size()
    }

    pub fn tell(&self) -> usize {
        self.global_offset
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }

    pub fn read<T>(&mut self) -> T {
        assert!(self.global_offset + mem::size_of::<T>() <= self.total_size);
        let mut buf = vec![0; mem::size_of::<T>()];
        self.read_data(&mut buf);
        unsafe { ptr::read_unaligned(buf.as_ptr().cast()) }
    }

    pub fn read_data(&mut self, buf: &mut [u8]) {
        assert!(self.global_offset + buf.len() <= self.total_size);
        let mut buf = buf;
        while buf.len() > 0 {
            let current_slice_ref = unsafe { self.current_slice.as_ref() };
            if self.current_slice_offset == current_slice_ref.capacity() {
                self.current_slice = current_slice_ref.next_ptr();
                self.current_slice_offset = 0;
                continue;
            }
            let len = std::cmp::min(
                buf.len(),
                current_slice_ref.capacity() - self.current_slice_offset,
            );
            let data_slice = current_slice_ref.data_slice(self.current_slice_offset, len);
            buf[..len].copy_from_slice(data_slice);
            self.global_offset += len;
            self.current_slice_offset += len;
            buf = &mut buf[len..];
        }
    }

    pub fn seek(&mut self, offset: usize) {
        assert!(offset >= self.global_offset);
        if offset == self.global_offset {
            return;
        }
        let mut len = offset - self.global_offset;
        while len > 0 {
            let current_slice_ref = unsafe { self.current_slice.as_ref() };
            if self.current_slice_offset == current_slice_ref.capacity() {
                self.current_slice = current_slice_ref.next_ptr();
                self.current_slice_offset = 0;
                continue;
            }
            let len0 = std::cmp::min(
                len,
                current_slice_ref.capacity() - self.current_slice_offset,
            );
            self.current_slice_offset += len0;
            self.global_offset += len0;
            len -= len0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use byte_slice_list::ByteSliceReader;

    use crate::postings::byte_slice_list::{self, ByteSlice};

    use super::ByteSliceWriter;

    #[test]
    fn test_simple() {
        let writer: ByteSliceWriter = ByteSliceWriter::with_initial_capacity(4);
        let head_ptr = writer.byte_slice_list.head;
        let head = writer.head();
        let data = vec![1, 2];

        writer.write_data(&data);
        assert_eq!(writer.total_size(), 2);
        assert_eq!(head.data(), data);

        let data = vec![3, 4];
        writer.write_data(&data);
        assert_eq!(writer.total_size(), 4);
        let data = vec![1, 2, 3, 4];
        assert_eq!(head.data(), data);
        assert!(head.next.load().is_null());

        let tail_ptr = writer.tail.get();
        assert_eq!(head_ptr, tail_ptr);

        let data = vec![5, 6, 7, 8, 9];
        writer.write_data(&data);
        assert_eq!(writer.total_size(), 9);

        let data = vec![1, 2, 3, 4];
        assert_eq!(head.data(), data);

        let next = head.next();
        let data = vec![5, 6, 7, 8];
        assert_eq!(next.data(), data);

        let next = next.next();
        let data = vec![9];
        assert_eq!(next.data(), data);

        let tail_ptr = writer.tail.get();
        let last_ptr = next as *const ByteSlice;
        assert_eq!(last_ptr, tail_ptr.as_ptr());
    }

    #[test]
    fn test_multithreads() {
        let writer: ByteSliceWriter = ByteSliceWriter::with_initial_capacity(4);
        let byte_slice_list = writer.byte_slice_list();
        thread::scope(|scope| {
            let t = scope.spawn(move || loop {
                let total_size = byte_slice_list.total_size();
                if total_size == 0 {
                    continue;
                }

                let mut offset = 0;
                let mut next_slice = byte_slice_list.head();
                loop {
                    offset += next_slice.size();
                    // offset may greater than total_size,
                    // when new data was written after read total_size
                    if offset >= total_size {
                        break;
                    }
                    next_slice = next_slice.next();
                }

                if total_size == 9 {
                    break;
                }
            });

            let head_ptr = writer.byte_slice_list.head;
            let head = writer.head();
            let data = vec![1, 2];

            writer.write_data(&data);
            assert_eq!(writer.total_size(), 2);
            assert_eq!(head.data(), data);

            let data = vec![3, 4];
            writer.write_data(&data);
            assert_eq!(writer.total_size(), 4);
            let data = vec![1, 2, 3, 4];
            assert_eq!(head.data(), data);
            assert!(head.next.load().is_null());

            let tail_ptr = writer.tail.get();
            assert_eq!(head_ptr, tail_ptr);

            let data = vec![5, 6, 7, 8, 9];
            writer.write_data(&data);
            assert_eq!(writer.total_size(), 9);

            let data = vec![1, 2, 3, 4];
            assert_eq!(head.data(), data);

            let next = head.next();
            let data = vec![5, 6, 7, 8];
            assert_eq!(next.data(), data);

            let next = next.next();
            let data = vec![9];
            assert_eq!(next.data(), data);

            let tail_ptr = writer.tail.get();
            let last_ptr = next as *const ByteSlice;
            assert_eq!(last_ptr, tail_ptr.as_ptr());

            t.join().unwrap();
        });
    }

    #[test]
    fn test_write_read() {
        let writer: ByteSliceWriter = ByteSliceWriter::with_initial_capacity(4);
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        writer.write_data(&data);
        assert_eq!(writer.total_size(), 10);

        let byte_slice_list = writer.byte_slice_list();
        let mut reader = ByteSliceReader::open(byte_slice_list);
        assert_eq!(reader.total_size, 10);
        assert_eq!(reader.global_offset, 0);

        let mut buf: Vec<u8> = vec![0; 2];
        reader.read_data(&mut buf[..]);
        assert_eq!(&buf[..], &[1, 2]);
        assert_eq!(reader.global_offset, 2);
        assert_eq!(reader.current_slice_offset, 2);

        let mut buf: Vec<u8> = vec![0; 7];
        reader.read_data(&mut buf[..]);
        assert_eq!(&buf[..], &[3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(reader.global_offset, 9);
        assert_eq!(reader.current_slice_offset, 1);
    }

    #[test]
    fn test_write_read_multithreads() {
        let writer: ByteSliceWriter = ByteSliceWriter::with_initial_capacity(4);
        let byte_slice_list = writer.byte_slice_list();

        thread::scope(|scope| {
            let t = scope.spawn(move || loop {
                let mut reader = ByteSliceReader::open(byte_slice_list);
                if reader.total_size != 10 {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                let mut buf: Vec<u8> = vec![0; 2];
                reader.read_data(&mut buf[..]);
                assert_eq!(&buf[..], &[1, 2]);
                assert_eq!(reader.global_offset, 2);
                assert_eq!(reader.current_slice_offset, 2);

                let mut buf: Vec<u8> = vec![0; 7];
                reader.read_data(&mut buf[..]);
                assert_eq!(&buf[..], &[3, 4, 5, 6, 7, 8, 9]);
                assert_eq!(reader.global_offset, 9);
                assert_eq!(reader.current_slice_offset, 1);

                break;
            });

            let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            writer.write_data(&data);
            assert_eq!(writer.total_size(), 10);

            t.join().unwrap();
        });
    }
}
