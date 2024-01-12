use std::io;

pub fn copy_write<T, W: io::Write>(src: &[T], writer: &mut W) -> usize {
    let data = unsafe {
        std::slice::from_raw_parts(
            src.as_ptr() as *const u8,
            src.len() * std::mem::size_of::<T>(),
        )
    };
    writer.write_all(data).unwrap();

    data.len()
}

pub fn copy_read<T, R: io::Read>(reader: &mut R, dst: &mut [T]) {
    let mut data = unsafe {
        std::slice::from_raw_parts_mut(
            dst.as_mut_ptr() as *mut u8,
            dst.len() * std::mem::size_of::<T>(),
        )
    };

    reader.read_exact(&mut data).unwrap();
}
