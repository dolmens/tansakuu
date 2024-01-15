use std::io::{self, Read};

use crate::{postings::PostingEncoder, SKIPLIST_BLOCK_LEN};

use super::{SkipListBlock, SkipListFormat};

pub trait SkipListSeek {
    // return (offset, last_key, skip_count)
    fn seek(&mut self, key: u32) -> io::Result<(usize, u32, usize)>;
}

pub struct SkipListReader<R: Read> {
    skip_count: usize,
    read_count: usize,
    item_count: usize,
    last_key: u32,
    current_offset: usize,
    current_cursor: usize,
    skip_list_block: SkipListBlock,
    reader: R,
    skip_list_format: SkipListFormat,
}

impl<R: Read> SkipListReader<R> {
    pub fn open(skip_list_format: SkipListFormat, item_count: usize, reader: R) -> Self {
        Self {
            skip_count: 0,
            read_count: 0,
            item_count,
            last_key: 0,
            current_offset: 0,
            current_cursor: 0,
            skip_list_block: SkipListBlock::new(&skip_list_format),
            reader,
            skip_list_format,
        }
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.item_count && self.current_cursor >= self.skip_list_block.len
    }

    fn decode_one_block(&mut self) -> io::Result<()> {
        self.skip_list_block.len = 0;
        if self.eof() {
            return Ok(());
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

        Ok(())
    }
}

impl<R: Read> SkipListSeek for SkipListReader<R> {
    fn seek(&mut self, key: u32) -> io::Result<(usize, u32, usize)> {
        if self.eof() {
            return Ok((self.current_offset, self.last_key, self.skip_count));
        }

        loop {
            if self.current_cursor == self.skip_list_block.len {
                self.decode_one_block()?;
                if self.skip_list_block.len == 0 {
                    break;
                }
            }
            if self.last_key + self.skip_list_block.keys[self.current_cursor] >= key {
                break;
            }
            self.last_key += self.skip_list_block.keys[self.current_cursor];
            self.current_offset += self.skip_list_block.offsets[self.current_cursor] as usize;
            self.current_cursor += 1;
            self.skip_count += 1;
        }

        Ok((self.current_offset, self.last_key, self.skip_count))
    }
}

pub struct NoSkipList;

impl SkipListSeek for NoSkipList {
    fn seek(&mut self, _key: u32) -> io::Result<(usize, u32, usize)> {
        Ok((0, 0, 0))
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            skiplist::{
                skip_list_reader::SkipListReader, skip_list_reader::SkipListSeek, SkipListFormat,
            },
            PostingEncoder,
        },
        SKIPLIST_BLOCK_LEN,
    };

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
            .map(|(i, _)| (100 + i % 8) as u32)
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

        let (offset, last_key, skipped) = reader.seek(0)?;
        assert_eq!(offset, 0);
        assert_eq!(last_key, 0);
        assert_eq!(skipped, 0);

        for (i, &key) in keys.iter().enumerate().skip(1) {
            let (offset, last_key, skipped) = reader.seek(key)?;
            assert_eq!(offset, total_offsets[i - 1]);
            assert_eq!(last_key, keys[i - 1]);
            assert_eq!(skipped, i);
        }

        let (offset, last_key, skipped) = reader.seek(keys.last().cloned().unwrap() + 1)?;
        assert_eq!(offset, total_offsets.last().cloned().unwrap());
        assert_eq!(last_key, keys.last().cloned().unwrap());
        assert_eq!(skipped, BLOCK_LEN * 2 + 3);

        let (offset, last_key, skipped) = reader.seek(keys.last().cloned().unwrap() + 2)?;
        assert_eq!(offset, total_offsets.last().cloned().unwrap());
        assert_eq!(last_key, keys.last().cloned().unwrap());
        assert_eq!(skipped, BLOCK_LEN * 2 + 3);

        let buf_reader = BufReader::new(buf.as_slice());
        let mut reader =
            SkipListReader::open(skip_list_format.clone(), BLOCK_LEN * 2 + 3, buf_reader);
        let (offset, last_key, skipped) = reader.seek(keys.last().cloned().unwrap())?;
        assert_eq!(offset, total_offsets.iter().rev().nth(1).cloned().unwrap());
        assert_eq!(last_key, keys.iter().rev().nth(1).cloned().unwrap());
        assert_eq!(skipped, total_offsets.len() - 1);

        Ok(())
    }
}
