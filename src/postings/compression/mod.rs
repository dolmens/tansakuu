use allocator_api2::alloc::Allocator;

use super::{ByteSliceReader, ByteSliceWriter};

pub fn copy_write<T, A: Allocator>(src: &[T], slice_writer: &mut ByteSliceWriter<A>) -> usize {
    let data = unsafe {
        std::slice::from_raw_parts(
            src.as_ptr() as *const u8,
            src.len() * std::mem::size_of::<T>(),
        )
    };
    slice_writer.write_data(data);
    data.len()
}

pub fn copy_read<T>(slice_reader: &mut ByteSliceReader, dst: &mut [T]) {
    let data = unsafe {
        std::slice::from_raw_parts_mut(
            dst.as_mut_ptr() as *mut u8,
            dst.len() * std::mem::size_of::<T>(),
        )
    };
    slice_reader.read_data(data);
}
