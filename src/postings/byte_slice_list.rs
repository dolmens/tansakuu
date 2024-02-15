use std::{
    alloc::Layout,
    io::{self, Read, Write},
    ptr::{self, NonNull},
    slice,
    sync::Arc,
};

use allocator_api2::alloc::{Allocator, Global};

use crate::util::{
    atomic::{AcqRelUsize, RelaxedAtomicPtr},
    capacity_policy::CapacityPolicy,
    fractional_capacity_policy::FractionalChunkCapacityPolicy,
};

pub struct ByteSlice {
    next: RelaxedAtomicPtr<ByteSlice>,
    capacity: usize,
    data: NonNull<u8>,
}

pub struct ByteSliceList<A: Allocator = Global> {
    total_size: AcqRelUsize,
    head: RelaxedAtomicPtr<ByteSlice>,
    allocator: A,
}

pub struct ByteSliceWriter<C: CapacityPolicy = FractionalChunkCapacityPolicy, A: Allocator = Global>
{
    total_size: usize,
    current_slice_offset: usize,
    current_slice: Option<NonNull<ByteSlice>>,
    byte_slice_list: Arc<ByteSliceList<A>>,
    capacity_policy: C,
}

unsafe impl<C: CapacityPolicy, A: Allocator> Send for ByteSliceWriter<C, A> {}

pub struct ByteSliceReader<'a> {
    total_size: usize,
    global_offset: usize,
    current_slice_offset: usize,
    current_slice: Option<&'a ByteSlice>,
}

impl ByteSlice {
    fn new(capacity: usize, data: NonNull<u8>) -> Self {
        Self {
            next: RelaxedAtomicPtr::new(ptr::null_mut()),
            capacity,
            data,
        }
    }

    pub fn next(&self) -> Option<&ByteSlice> {
        unsafe { self.next.load().as_ref() }
    }

    pub fn data_slice(&self, offset: usize, len: usize) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data.as_ptr().add(offset), len) }
    }

    pub fn data_slice_mut(&self, offset: usize, len: usize) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.data.as_ptr().add(offset), len) }
    }
}

impl<A: Allocator> ByteSliceList<A> {
    fn new(allocator: A) -> Self {
        Self {
            total_size: AcqRelUsize::new(0),
            head: RelaxedAtomicPtr::default(),
            allocator,
        }
    }

    fn with_initial_capacity_in(initial_capacity: usize, allocator: A) -> Self {
        let head = Self::create_slice_in(initial_capacity, &allocator);

        Self {
            total_size: AcqRelUsize::new(0),
            head: RelaxedAtomicPtr::new(head.as_ptr()),
            allocator,
        }
    }

    fn create_slice_in(capacity: usize, allocator: &A) -> NonNull<ByteSlice> {
        let layout = Self::layout_with_capacity(capacity);
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

    fn layout_with_capacity(capacity: usize) -> Layout {
        Layout::new::<ByteSlice>()
            .extend(Layout::from_size_align(capacity, 1).unwrap())
            .unwrap()
            .0
    }

    fn create_slice(&self, capacity: usize) -> NonNull<ByteSlice> {
        Self::create_slice_in(capacity, &self.allocator)
    }

    pub fn total_size(&self) -> usize {
        self.total_size.load()
    }

    pub fn head_ref(&self) -> Option<&ByteSlice> {
        unsafe { self.head.load().as_ref() }
    }
}

impl<A: Allocator> Drop for ByteSliceList<A> {
    fn drop(&mut self) {
        if self.total_size() == 0 {
            return;
        }
        let mut slice = self.head.load();
        while !slice.is_null() {
            let slice_ref = unsafe { &*slice };
            let capacity = slice_ref.capacity;
            let next_silce = slice_ref.next.load();
            let layout = Self::layout_with_capacity(capacity);
            unsafe {
                self.allocator
                    .deallocate(NonNull::new_unchecked(slice).cast(), layout);
            }
            slice = next_silce;
        }
    }
}

impl<C: CapacityPolicy + Default, A: Allocator + Default> ByteSliceWriter<C, A> {
    pub fn new() -> Self {
        Self::new_in(A::default())
    }

    pub fn with_initial_capacity(initial_capacity: usize) -> Self {
        Self::with_initial_capacity_in(initial_capacity, A::default())
    }
}

impl<C: CapacityPolicy + Default, A: Allocator> ByteSliceWriter<C, A> {
    pub fn new_in(allocator: A) -> Self {
        let byte_slice_list = Arc::new(ByteSliceList::new(allocator));

        Self {
            total_size: 0,
            current_slice_offset: 0,
            current_slice: None,
            byte_slice_list,
            capacity_policy: Default::default(),
        }
    }

    pub fn with_initial_capacity_in(initial_capacity: usize, allocator: A) -> Self {
        let byte_slice_list = Arc::new(ByteSliceList::with_initial_capacity_in(
            initial_capacity,
            allocator,
        ));
        let current_slice = NonNull::new(byte_slice_list.head.load());

        Self {
            total_size: 0,
            current_slice_offset: 0,
            current_slice,
            byte_slice_list,
            capacity_policy: Default::default(),
        }
    }
}

impl<C: CapacityPolicy, A: Allocator> ByteSliceWriter<C, A> {
    pub fn byte_slice_list(&self) -> Arc<ByteSliceList<A>> {
        self.byte_slice_list.clone()
    }

    fn add_slice(&mut self) {
        let next_slice_capacity = self.get_next_slice_capcacity(self.current_slice_capacity());
        assert!(next_slice_capacity > 0);
        let next_byte_slice = self.byte_slice_list.create_slice(next_slice_capacity);
        if let Some(current_slice) = self.current_slice_ref() {
            current_slice.next.store(next_byte_slice.as_ptr());
        } else {
            self.byte_slice_list.head.store(next_byte_slice.as_ptr());
        }
        self.current_slice = Some(next_byte_slice);
        self.current_slice_offset = 0;
    }

    fn current_slice_ref(&self) -> Option<&ByteSlice> {
        self.current_slice.map(|slice| unsafe { slice.as_ref() })
    }

    fn current_slice_capacity(&self) -> usize {
        self.current_slice_ref().map_or(0, |slice| slice.capacity)
    }

    fn current_slice_is_full(&self) -> bool {
        self.current_slice_offset == self.current_slice_capacity()
    }

    fn get_next_slice_capcacity(&self, current_slice_capacity: usize) -> usize {
        self.capacity_policy.next_capacity(current_slice_capacity)
    }
}

impl<C: CapacityPolicy, A: Allocator> Write for ByteSliceWriter<C, A> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.current_slice_is_full() {
            self.add_slice();
        }
        let current_slice = self.current_slice_ref().unwrap();
        let remain_space = current_slice.capacity - self.current_slice_offset;
        let size_to_write = std::cmp::min(remain_space, buf.len());
        let data_dst = current_slice.data_slice_mut(self.current_slice_offset, size_to_write);
        data_dst.copy_from_slice(&buf[..size_to_write]);

        self.current_slice_offset += size_to_write;
        self.total_size += size_to_write;
        self.byte_slice_list.total_size.store(self.total_size);

        Ok(size_to_write)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> ByteSliceReader<'a> {
    pub fn empty() -> Self {
        Self {
            total_size: 0,
            global_offset: 0,
            current_slice_offset: 0,
            current_slice: None,
        }
    }

    pub fn open<A: Allocator>(byte_slice_list: &'a ByteSliceList<A>) -> Self {
        Self {
            total_size: byte_slice_list.total_size(),
            global_offset: 0,
            current_slice_offset: 0,
            current_slice: byte_slice_list.head_ref(),
        }
    }

    pub fn eof(&self) -> bool {
        self.tell() == self.total_size()
    }

    pub fn current_slice_eof(&self) -> bool {
        self.current_slice_offset == self.current_slice_capacity()
    }

    pub fn current_slice_capacity(&self) -> usize {
        self.current_slice.map_or(0, |slice| slice.capacity)
    }

    pub fn tell(&self) -> usize {
        self.global_offset
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }

    pub fn remain_size(&self) -> usize {
        self.total_size - self.global_offset
    }
}

impl<'a> Read for ByteSliceReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.eof() {
            return Ok(0);
        }
        if self.current_slice_eof() {
            self.current_slice = self.current_slice.unwrap().next();
            self.current_slice_offset = 0;
        }
        let current_slice = self.current_slice.unwrap();
        let max_remain_size = current_slice.capacity - self.current_slice_offset;
        let total_remain_size = self.remain_size();
        let remain_size = std::cmp::min(max_remain_size, total_remain_size);
        let size = std::cmp::min(buf.len(), remain_size);
        let data_slice = current_slice.data_slice(self.current_slice_offset, size);
        buf[..size].copy_from_slice(data_slice);
        self.current_slice_offset += size;
        self.global_offset += size;

        return Ok(size);
    }
}

impl<'a> io::Seek for ByteSliceReader<'a> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match pos {
            io::SeekFrom::Start(offset) => {
                let offset = offset as usize;
                if offset > self.global_offset {
                    let mut len = offset - self.global_offset;
                    while len > 0 {
                        let current_slice = self.current_slice.unwrap();
                        if self.current_slice_offset == current_slice.capacity {
                            self.current_slice = current_slice.next();
                            self.current_slice_offset = 0;
                            continue;
                        }
                        let len0 =
                            std::cmp::min(len, current_slice.capacity - self.current_slice_offset);
                        self.current_slice_offset += len0;
                        self.global_offset += len0;
                        len -= len0;
                    }
                }
                Ok(self.global_offset as u64)
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read,
        io::{self, Write},
        ptr::NonNull,
        thread,
        time::Duration,
    };

    use crate::{
        postings::{byte_slice_list::ByteSlice, ByteSliceReader},
        util::capacity_policy::FixedCapacityPolicy,
    };

    use super::ByteSliceWriter;

    #[test]
    fn test_new_empty() -> io::Result<()> {
        let writer: ByteSliceWriter = ByteSliceWriter::new();
        let byte_slice_list = writer.byte_slice_list();
        let mut reader = ByteSliceReader::open(&byte_slice_list);
        let mut buf = vec![0];
        assert_eq!(reader.read(&mut buf)?, 0);
        assert!(reader.read_exact(&mut buf).is_err());

        Ok(())
    }

    #[test]
    fn test_add_slice_basic() -> io::Result<()> {
        let mut writer: ByteSliceWriter = ByteSliceWriter::new();
        let byte_slice_list = writer.byte_slice_list();
        let reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 0);
        assert!(reader.current_slice.is_none());
        let data = vec![1, 2];
        writer.write_all(&data)?;
        let mut reader = ByteSliceReader::open(&byte_slice_list);
        let mut buf = vec![0; 2];
        reader.read_exact(&mut buf)?;
        assert_eq!(data, buf);

        Ok(())
    }

    #[test]
    fn test_add_slice_multithread() -> io::Result<()> {
        let mut writer: ByteSliceWriter = ByteSliceWriter::new();
        let byte_slice_list = writer.byte_slice_list();
        let data = vec![1, 2];
        thread::scope(|scope| {
            let rt = scope.spawn(|| loop {
                let mut reader = ByteSliceReader::open(&byte_slice_list);
                if reader.total_size() < 2 {
                    thread::yield_now();
                    continue;
                }
                let mut buf = vec![0; 2];
                reader.read_exact(&mut buf).unwrap();
                assert_eq!(data, buf);
                break;
            });
            let wt = scope.spawn(|| {
                writer.write_all(&data).unwrap();
            });

            rt.join().unwrap();
            wt.join().unwrap();
        });

        Ok(())
    }

    #[test]
    fn test_simple_writer_only() {
        let mut writer: ByteSliceWriter<FixedCapacityPolicy> =
            ByteSliceWriter::with_initial_capacity(4);
        assert_eq!(writer.total_size, 0);
        let byte_slice_list = writer.byte_slice_list();

        let head_ptr = NonNull::new(byte_slice_list.head.load());
        let head = byte_slice_list.head_ref().unwrap();

        let data = vec![1, 2];

        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 2);
        assert_eq!(head.data_slice(0, 2), data);

        let data = vec![3, 4];
        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 4);
        let data = vec![1, 2, 3, 4];
        assert_eq!(head.data_slice(0, 4), data);

        let tail_ptr = writer.current_slice;
        assert_eq!(head_ptr, tail_ptr);

        let data = vec![5, 6, 7, 8, 9];
        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 9);

        let data = vec![1, 2, 3, 4];
        assert_eq!(head.data_slice(0, 4), data);

        let next = head.next().unwrap();
        let data = vec![5, 6, 7, 8];
        assert_eq!(next.data_slice(0, 4), data);

        let next = next.next().unwrap();
        let data = vec![9];
        assert_eq!(next.data_slice(0, 1), data);

        let tail_ptr = writer.current_slice.unwrap();
        let last_ptr = next as *const ByteSlice;
        assert_eq!(last_ptr, tail_ptr.as_ptr());
    }

    #[test]
    fn test_multithreads() {
        let mut writer: ByteSliceWriter<FixedCapacityPolicy> =
            ByteSliceWriter::with_initial_capacity(4);
        let byte_slice_list = writer.byte_slice_list();
        thread::scope(|scope| {
            let t = scope.spawn(move || {
                let mut total_size = 0;
                loop {
                    let current_total_size = byte_slice_list.total_size();
                    if current_total_size == total_size {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }

                    total_size = current_total_size;
                    let mut buf = vec![];

                    let mut offset = 0;
                    let mut slice = byte_slice_list.head_ref().unwrap();
                    loop {
                        let len = std::cmp::min(slice.capacity, total_size - offset);
                        buf.extend_from_slice(slice.data_slice(0, len));
                        offset += len;
                        if offset == total_size {
                            break;
                        }
                        slice = slice.next().unwrap();
                    }
                    let data: &[u8] = &(1..=total_size as u8).collect::<Vec<u8>>();
                    assert_eq!(&buf, data);

                    if total_size == 9 {
                        break;
                    }
                }
            });

            let byte_slice_list = writer.byte_slice_list();
            let head_ptr = NonNull::new(byte_slice_list.head.load());
            let head = byte_slice_list.head_ref().unwrap();
            let data = vec![1, 2];

            writer.write_all(&data).unwrap();
            assert_eq!(writer.total_size, 2);
            assert_eq!(head.data_slice(0, 2), data);

            let data = vec![3, 4];
            writer.write_all(&data).unwrap();
            assert_eq!(writer.total_size, 4);
            let data = vec![1, 2, 3, 4];
            assert_eq!(head.data_slice(0, 4), data);

            let tail_ptr = writer.current_slice;
            assert_eq!(head_ptr, tail_ptr);

            let data = vec![5, 6, 7, 8, 9];
            writer.write_all(&data).unwrap();
            assert_eq!(writer.total_size, 9);

            let data = vec![1, 2, 3, 4];
            assert_eq!(head.data_slice(0, 4), data);

            let next = head.next().unwrap();
            let data = vec![5, 6, 7, 8];
            assert_eq!(next.data_slice(0, 4), data);

            let next = next.next().unwrap();
            let data = vec![9];
            assert_eq!(next.data_slice(0, 1), data);

            let tail_ptr = writer.current_slice.unwrap();
            let last_ptr = next as *const ByteSlice;
            assert_eq!(last_ptr, tail_ptr.as_ptr());

            t.join().unwrap();
        });
    }

    #[test]
    fn test_simple_writer_and_reader() {
        let mut writer: ByteSliceWriter = ByteSliceWriter::with_initial_capacity(4);
        assert_eq!(writer.total_size, 0);
        let byte_slice_list = writer.byte_slice_list();
        let reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 0);

        let data = vec![1, 2];

        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 2);

        let mut reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 2);
        let mut buf = vec![0; 1];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data[0..1]);
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data[1..2]);

        let mut reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 2);
        let mut buf = vec![0; 2];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data);

        let data = vec![3, 4];
        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 4);
        let data = vec![1, 2, 3, 4];

        let mut reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 4);
        let mut buf = vec![0; 4];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data);

        let data = vec![5, 6, 7, 8, 9];
        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 9);

        let mut reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 9);

        let mut buf = vec![0; 6];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[1, 2, 3, 4, 5, 6]);

        let mut buf = vec![0; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[7, 8, 9]);
    }

    #[test]
    fn test_multithreads_writer_and_reader() {
        let mut writer: ByteSliceWriter = ByteSliceWriter::with_initial_capacity(4);
        assert_eq!(writer.total_size, 0);
        let byte_slice_list = writer.byte_slice_list();

        let th = thread::spawn(move || {
            let mut total_size = 0;
            loop {
                let mut reader = ByteSliceReader::open(&byte_slice_list);
                if reader.total_size() == total_size {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }

                total_size = reader.total_size();
                let mut buf = vec![0; total_size];
                reader.read_exact(&mut buf).unwrap();
                let data: &[u8] = &(1..=total_size as u8).collect::<Vec<u8>>();
                assert_eq!(&buf, data);

                if total_size == 9 {
                    break;
                }
            }
        });

        let byte_slice_list = writer.byte_slice_list();
        let reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 0);

        let data = vec![1, 2];

        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 2);

        let mut reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 2);
        let mut buf = vec![0; 1];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data[0..1]);
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data[1..2]);

        let mut reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 2);
        let mut buf = vec![0; 2];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data);

        let data = vec![3, 4];
        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 4);
        let data = vec![1, 2, 3, 4];

        let mut reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 4);
        let mut buf = vec![0; 4];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data);

        let data = vec![5, 6, 7, 8, 9];
        writer.write_all(&data).unwrap();
        assert_eq!(writer.total_size, 9);

        let mut reader = ByteSliceReader::open(&byte_slice_list);
        assert_eq!(reader.total_size(), 9);

        let mut buf = vec![0; 6];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[1, 2, 3, 4, 5, 6]);

        let mut buf = vec![0; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[7, 8, 9]);

        th.join().unwrap();
    }
}
