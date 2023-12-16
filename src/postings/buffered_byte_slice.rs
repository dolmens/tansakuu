use std::slice;

const INITIAL_SLICE_CAPACITY: usize = 128;

use crate::postings::multi_value_buffer::ValueItem;

use super::{multi_value_buffer::MultiValue, ByteSliceReader, ByteSliceWriter, MultiValueBuffer};

pub struct BufferedByteSlice {
    multi_value_buffer: MultiValueBuffer,
    byte_slice_writer: ByteSliceWriter,
}

pub struct BufferedByteSliceReader<'a> {
    eof: bool,
    multi_value_buffer: MultiValueBuffer,
    byte_slice_reader: ByteSliceReader<'a>,
}

pub fn transmute_mut_slice<T>(buf: &mut [T]) -> &mut [u8] {
    unsafe {
        std::slice::from_raw_parts_mut(
            buf.as_mut_ptr() as *mut u8,
            buf.len() * std::mem::size_of::<T>(),
        )
    }
}

impl BufferedByteSlice {
    pub fn new(multi_value: MultiValue, buffer_capacity: usize) -> Self {
        let multi_value_buffer = MultiValueBuffer::new(multi_value, buffer_capacity);
        let byte_slice_writer = ByteSliceWriter::with_initial_capacity(INITIAL_SLICE_CAPACITY);

        Self {
            multi_value_buffer,
            byte_slice_writer,
        }
    }

    pub fn multi_value_buffer(&self) -> &MultiValueBuffer {
        &self.multi_value_buffer
    }

    pub fn byte_slice_writer(&self) -> &ByteSliceWriter {
        &self.byte_slice_writer
    }

    pub fn reader(&self) -> BufferedByteSliceReader<'_> {
        BufferedByteSliceReader::new(self)
    }

    pub fn push<T>(&mut self, row: usize, value: T) {
        self.multi_value_buffer.push(row, value);
    }

    pub fn end_push(&mut self) -> usize {
        self.multi_value_buffer.end_push();
        if self.need_flush() {
            self.flush()
        } else {
            0
        }
    }

    fn need_flush(&self) -> bool {
        self.multi_value_buffer.is_full()
    }

    pub fn flush(&mut self) -> usize {
        let mut encoded_len = 0;
        let len = self.multi_value_buffer.len();
        for (row, value_item) in self
            .multi_value_buffer
            .value_items()
            .value_items()
            .iter()
            .enumerate()
        {
            let row_data = self.multi_value_buffer.row_data(row);
            let row_data_slice =
                unsafe { slice::from_raw_parts(row_data.as_ptr(), value_item.size * len) };
            encoded_len += value_item.encode(row_data_slice, &self.byte_slice_writer);
        }
        self.multi_value_buffer.clear();
        encoded_len
    }
}

impl<'a> BufferedByteSliceReader<'a> {
    pub fn new(buffered_byte_slice: &'a BufferedByteSlice) -> Self {
        let byte_slice_list = buffered_byte_slice.byte_slice_writer().byte_slice_list();
        let mut byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        let multi_value_buffer = buffered_byte_slice.multi_value_buffer().snapshot();

        if byte_slice_list.total_size() > byte_slice_reader.total_size() {
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
            multi_value_buffer.clear();
        }

        Self {
            eof: false,
            byte_slice_reader,
            multi_value_buffer,
        }
    }

    // return item count
    pub fn decode_one_block(&mut self, buffers: &mut [&mut [u8]]) -> usize {
        if self.eof {
            return 0;
        }
        assert_eq!(buffers.len(), self.multi_value().value_items_count());
        if self.byte_slice_reader.eof() {
            let len = self.multi_value_buffer.len();
            if len > 0 {
                for (i, value_item) in self.multi_value().value_items().iter().enumerate() {
                    assert!(len * value_item.size <= buffers[i].len());
                    let row_data = self.multi_value_buffer.row_data(i);
                    let row_slice =
                        unsafe { slice::from_raw_parts(row_data.as_ptr(), len * value_item.size) };
                    buffers[i][..len * value_item.size].copy_from_slice(row_slice);
                }
            }
            self.eof = true;
            len
        } else {
            let value_items: Vec<ValueItem> =
                self.multi_value().value_items().iter().cloned().collect();
            let mut decode_lens = vec![];
            for (i, value_item) in value_items.iter().enumerate() {
                decode_lens.push(value_item.decode(&mut self.byte_slice_reader, buffers[i]));
            }
            decode_lens[0]
        }
    }

    pub fn multi_value(&self) -> &MultiValue {
        self.multi_value_buffer.value_items()
    }

    pub fn eof(&self) -> bool {
        self.eof
    }
}

#[cfg(test)]
mod tests {
    use crate::postings::{copy_decode, copy_encode, transmute_mut_slice, MultiValue};

    use super::BufferedByteSlice;

    #[test]
    fn test_simple() {
        let mut value_items = MultiValue::new();
        value_items.add_value::<i8>(copy_encode, copy_decode);
        value_items.add_value::<i64>(copy_encode, copy_decode);
        value_items.add_value::<i32>(copy_encode, copy_decode);
        let buffer_capacity = 4;
        let mut writer = BufferedByteSlice::new(value_items, buffer_capacity);

        let row0: Vec<i8> = (1..100).collect();
        let row1: Vec<i64> = (100..200).collect();
        let row2: Vec<i32> = (200..300).collect();

        let mut decode0 = vec![0i8; buffer_capacity];
        let mut decode1 = vec![0i64; buffer_capacity];
        let mut decode2 = vec![0i32; buffer_capacity];

        let mut decode_buffers = vec![
            transmute_mut_slice(&mut decode0[..]),
            transmute_mut_slice(&mut decode1[..]),
            transmute_mut_slice(&mut decode2[..]),
        ];

        let mut col = 0;

        while col < 3 {
            writer.push::<i8>(0, row0[col]);
            writer.push::<i64>(1, row1[col]);
            writer.push::<i32>(2, row2[col]);
            assert_eq!(writer.end_push(), 0);

            col += 1;
        }

        let mut reader = writer.reader();
        let count = reader.decode_one_block(&mut decode_buffers[..]);
        assert_eq!(count, 3);
        assert_eq!(&row0[..3], &decode0[..3]);
        assert_eq!(&row1[..3], &decode1[..3]);
        assert_eq!(&row2[..3], &decode2[..3]);

        let mut decode_buffers = vec![
            transmute_mut_slice(&mut decode0[..]),
            transmute_mut_slice(&mut decode1[..]),
            transmute_mut_slice(&mut decode2[..]),
        ];

        // already eof
        let count = reader.decode_one_block(&mut decode_buffers[..]);
        assert_eq!(count, 0);

        writer.push::<i8>(0, row0[col]);
        writer.push::<i64>(1, row1[col]);
        writer.push::<i32>(2, row2[col]);
        assert_eq!(writer.end_push(), ((8 + 4) + (8 + 8 * 4) + (8 + 4 * 4)));

        col += 1;

        writer.push::<i8>(0, row0[col]);
        writer.push::<i64>(1, row1[col]);
        writer.push::<i32>(2, row2[col]);
        assert_eq!(writer.end_push(), 0);

        let mut reader = writer.reader();
        let mut decode_buffers = vec![
            transmute_mut_slice(&mut decode0[..]),
            transmute_mut_slice(&mut decode1[..]),
            transmute_mut_slice(&mut decode2[..]),
        ];
        let count = reader.decode_one_block(&mut decode_buffers[..]);
        assert_eq!(count, 4);
        assert_eq!(&row0[..4], &decode0[..4]);
        assert_eq!(&row1[..4], &decode1[..4]);
        assert_eq!(&row2[..4], &decode2[..4]);

        let mut decode_buffers = vec![
            transmute_mut_slice(&mut decode0[..]),
            transmute_mut_slice(&mut decode1[..]),
            transmute_mut_slice(&mut decode2[..]),
        ];
        let count = reader.decode_one_block(&mut decode_buffers[..]);
        assert_eq!(count, 1);
        assert_eq!(&row0[4..5], &decode0[..1]);
        assert_eq!(&row1[4..5], &decode1[..1]);
        assert_eq!(&row2[4..5], &decode2[..1]);

        let mut decode_buffers = vec![
            transmute_mut_slice(&mut decode0[..]),
            transmute_mut_slice(&mut decode1[..]),
            transmute_mut_slice(&mut decode2[..]),
        ];
        let count = reader.decode_one_block(&mut decode_buffers[..]);
        assert_eq!(count, 0);
    }
}
