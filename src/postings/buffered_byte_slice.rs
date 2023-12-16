use std::slice;

const INITIAL_SLICE_CAPACITY: usize = 128;

use crate::postings::multi_value_buffer::ValueItem;

use super::{multi_value_buffer::MultiValue, ByteSliceReader, ByteSliceWriter, MultiValueBuffer};

pub struct BufferedByteSlice {
    multi_value_buffer: MultiValueBuffer,
    byte_slice_writer: ByteSliceWriter,
}

pub struct BufferedByteSliceReader<'a> {
    multi_value_buffer: MultiValueBuffer,
    byte_slice_reader: ByteSliceReader<'a>,
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

    pub fn need_flush(&self) -> bool {
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
            byte_slice_reader,
            multi_value_buffer,
        }
    }

    // return item count
    pub fn decode_one_block(&mut self, buffers: &mut [&mut [u8]]) -> usize {
        assert_eq!(buffers.len(), self.multi_value().value_items_count());
        if self.byte_slice_reader.eof() {
            let len = self.multi_value_buffer.len();
            for (i, value_item) in self.multi_value().value_items().iter().enumerate() {
                assert!(len * value_item.size <= buffers[i].len());
                let row_data = self.multi_value_buffer.row_data(i);
                let row_slice =
                    unsafe { slice::from_raw_parts(row_data.as_ptr(), len * value_item.size) };
                buffers[i][..len * value_item.size].copy_from_slice(row_slice);
            }
        } else {
            let value_items: Vec<ValueItem> = vec![];
            for (i, value_item) in value_items.iter().enumerate() {
                value_item.decode(&mut self.byte_slice_reader, buffers[i]);
            }
        }
        0
    }

    pub fn multi_value(&self) -> &MultiValue {
        self.multi_value_buffer.value_items()
    }
}
