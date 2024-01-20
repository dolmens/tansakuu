use std::io::{self, Read};

use crate::{postings::PostingEncoder, SKIPLIST_BLOCK_LEN};

use super::{SkipListBlock, SkipListFormat};

pub trait SkipListRead {
    // found, prev_key, block_last_key, start_offset, end_offset, skipped_item_count
    fn seek(&mut self, key: u64) -> io::Result<(bool, u64, u64, u64, u64, usize)>;
    fn prev_value(&self) -> u64;
    fn block_last_value(&self) -> u64;
}

pub struct SkipListReader<R: Read> {
    item_count: usize,
    read_count: usize,
    skipped_item_count: usize,
    current_key: u64,
    current_offset: u64,
    prev_value: u64,
    current_value: u64,
    current_cursor: usize,
    skip_list_block: SkipListBlock,
    reader: R,
    skip_list_format: SkipListFormat,
}

impl<R: Read> SkipListReader<R> {
    pub fn open(skip_list_format: SkipListFormat, item_count: usize, reader: R) -> Self {
        Self {
            item_count,
            read_count: 0,
            skipped_item_count: 0,
            current_key: 0,
            current_offset: 0,
            prev_value: 0,
            current_value: 0,
            current_cursor: 0,
            skip_list_block: SkipListBlock::new(&skip_list_format),
            reader,
            skip_list_format,
        }
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.item_count && self.current_cursor == self.skip_list_block.len
    }

    fn decode_one_block(&mut self) -> io::Result<bool> {
        self.skip_list_block.len = 0;
        if self.eof() {
            return Ok(false);
        }

        let skip_list_block = &mut self.skip_list_block;
        let block_len = std::cmp::min(self.item_count - self.read_count, SKIPLIST_BLOCK_LEN);
        skip_list_block.len = block_len;
        let posting_encoder = PostingEncoder;
        posting_encoder.decode_u32(&mut self.reader, &mut skip_list_block.keys[0..block_len])?;
        posting_encoder.decode_u32(&mut self.reader, &mut skip_list_block.offsets[0..block_len])?;
        if self.skip_list_format.has_value() {
            posting_encoder.decode_u32(
                &mut self.reader,
                &mut skip_list_block.values.as_deref_mut().unwrap()[0..block_len],
            )?;
        }
        self.current_cursor = 0;
        self.read_count += block_len;

        Ok(true)
    }
}

impl<R: Read> SkipListRead for SkipListReader<R> {
    fn seek(&mut self, key: u64) -> io::Result<(bool, u64, u64, u64, u64, usize)> {
        if self.eof() {
            return Ok((
                false,
                self.current_key,
                self.current_key,
                self.current_offset,
                self.current_offset,
                self.skipped_item_count,
            ));
        }

        loop {
            if self.current_cursor == self.skip_list_block.len {
                if !self.decode_one_block()? {
                    break;
                }
            }

            let prev_key = self.current_key;
            let current_offset = self.current_offset;
            self.prev_value = self.current_value;
            let skipped_item_count = self.skipped_item_count;

            self.current_key += self.skip_list_block.keys[self.current_cursor] as u64;
            self.current_offset += self.skip_list_block.offsets[self.current_cursor] as u64;
            self.current_value += self
                .skip_list_block
                .values
                .as_ref()
                .map_or(0, |values| values[self.current_cursor] as u64);
            self.skipped_item_count += 1;
            self.current_cursor += 1;

            if self.current_key >= key {
                return Ok((
                    true,
                    prev_key,
                    self.current_key,
                    current_offset,
                    self.current_offset,
                    skipped_item_count,
                ));
            }
        }

        Ok((
            false,
            self.current_key,
            self.current_key,
            self.current_offset,
            self.current_offset,
            self.skipped_item_count,
        ))
    }

    fn prev_value(&self) -> u64 {
        self.prev_value
    }

    fn block_last_value(&self) -> u64 {
        self.current_value
    }
}

#[cfg(test)]
pub use tests::MockSkipListReader;

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            skip_list::{
                skip_list_reader::SkipListRead, skip_list_reader::SkipListReader, SkipListFormat,
            },
            PostingEncoder,
        },
        SKIPLIST_BLOCK_LEN,
    };

    pub struct MockSkipListReader {
        current_key: u64,
        current_offset: u64,
        prev_value: u64,
        current_value: u64,
        current_cursor: usize,
        keys: Vec<u64>,
        offsets: Vec<u64>,
        values: Option<Vec<u64>>,
    }

    impl MockSkipListReader {
        pub fn new(keys: Vec<u64>, offsets: Vec<u64>, values: Option<Vec<u64>>) -> Self {
            assert_eq!(keys.len(), offsets.len());
            if let Some(svalues) = &values {
                assert_eq!(keys.len(), svalues.len());
            }

            Self {
                current_key: 0,
                current_offset: 0,
                prev_value: 0,
                current_value: 0,
                current_cursor: 0,
                keys,
                offsets,
                values,
            }
        }
    }

    impl SkipListRead for MockSkipListReader {
        fn seek(&mut self, key: u64) -> io::Result<(bool, u64, u64, u64, u64, usize)> {
            if self.keys.is_empty() {
                return Ok((false, 0, 0, 0, 0, 0));
            }

            loop {
                if self.current_cursor == self.keys.len() {
                    break;
                }
                let prev_key = self.current_key;
                let current_offset = self.current_offset;
                self.prev_value = self.current_value;

                self.current_key = self.keys[self.current_cursor];
                self.current_offset = self.offsets[self.current_cursor];
                self.current_value = self
                    .values
                    .as_ref()
                    .map_or(0, |values| values[self.current_cursor]);
                self.current_cursor += 1;

                if self.current_key >= key {
                    return Ok((
                        true,
                        prev_key,
                        self.current_key,
                        current_offset,
                        self.current_offset,
                        self.current_cursor - 1,
                    ));
                }
            }

            Ok((
                false,
                self.current_key,
                self.current_key,
                self.current_offset,
                self.current_offset,
                self.current_cursor,
            ))
        }

        fn prev_value(&self) -> u64 {
            self.prev_value
        }

        fn block_last_value(&self) -> u64 {
            self.current_key
        }
    }

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormat::builder().build();

        let keys: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| ((i + 1) * 1000 + i % 8) as u32)
            .collect();
        let keys = &keys[..];
        let offsets: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| ((i + 1) * 10) as u32)
            .collect();
        let offsets = &offsets[..];
        let keys_encoded: Vec<_> = std::iter::once(keys[0])
            .chain(keys.windows(2).map(|pair| pair[1] - pair[0]))
            .collect();
        let total_offsets: Vec<usize> = offsets
            .iter()
            .scan(0, |acc, &x| {
                *acc += x as usize;
                Some(*acc)
            })
            .collect();

        let mut buf = vec![];

        let posting_encoder = PostingEncoder;

        posting_encoder
            .encode_u32(&keys_encoded[0..BLOCK_LEN], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&offsets[0..BLOCK_LEN], &mut buf)
            .unwrap();

        posting_encoder
            .encode_u32(&keys_encoded[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&offsets[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();

        posting_encoder
            .encode_u32(&keys_encoded[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&offsets[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();

        let buf_reader = BufReader::new(buf.as_slice());
        let mut reader =
            SkipListReader::open(skip_list_format.clone(), BLOCK_LEN * 2 + 3, buf_reader);
        assert!(!reader.eof());
        assert_eq!(reader.item_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, 0);

        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            reader.seek(0)?;
        assert!(found);
        assert_eq!(prev_key, 0);
        assert_eq!(block_last_key, keys[0] as u64);
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, total_offsets[0] as u64);
        assert_eq!(skipped, 0);

        for (i, &key) in keys.iter().enumerate().skip(1) {
            let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
                reader.seek(key as u64)?;
            assert!(found);
            assert_eq!(prev_key, keys[i - 1] as u64);
            assert_eq!(block_last_key, keys[i] as u64);
            assert_eq!(start_offset, total_offsets[i - 1] as u64);
            assert_eq!(end_offset, total_offsets[i] as u64);
            assert_eq!(skipped, i);
        }

        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            reader.seek((keys.last().cloned().unwrap() + 1) as u64)?;
        assert!(!found);
        assert_eq!(prev_key, keys.last().cloned().unwrap() as u64);
        assert_eq!(block_last_key, keys.last().cloned().unwrap() as u64);
        assert_eq!(start_offset, total_offsets.last().cloned().unwrap() as u64);
        assert_eq!(end_offset, total_offsets.last().cloned().unwrap() as u64);
        assert_eq!(skipped, BLOCK_LEN * 2 + 3);

        let buf_reader = BufReader::new(buf.as_slice());
        let mut reader =
            SkipListReader::open(skip_list_format.clone(), BLOCK_LEN * 2 + 3, buf_reader);
        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            reader.seek(keys.last().cloned().unwrap() as u64)?;
        assert!(found);
        assert_eq!(prev_key, keys.iter().rev().nth(1).cloned().unwrap() as u64);
        assert_eq!(block_last_key, keys.last().cloned().unwrap() as u64);
        assert_eq!(
            start_offset,
            total_offsets.iter().rev().nth(1).cloned().unwrap() as u64
        );
        assert_eq!(end_offset, total_offsets.last().cloned().unwrap() as u64);
        assert_eq!(skipped, total_offsets.len() - 1);

        Ok(())
    }
}
