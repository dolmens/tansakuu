use super::{ByteSliceReader, ByteSliceWriter};

pub type Encode = fn(src: &[u8], dst: &ByteSliceWriter) -> usize;
pub type Decode = fn(src: &mut ByteSliceReader, dst: &mut [u8]) -> usize;

pub fn copy_encode(src: &[u8], dst: &ByteSliceWriter) -> usize {
    dst.write(src.len());
    dst.write_data(src);

    std::mem::size_of::<usize>() + src.len()
}

pub fn copy_decode(src: &mut ByteSliceReader, dst: &mut [u8]) -> usize {
    let len: usize = src.read();
    assert!(len <= dst.len());
    src.read_data(&mut dst[..len]);
    len
}
