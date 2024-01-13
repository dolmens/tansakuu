use std::io;

use crate::{postings::PostingEncoder, DocId, SKIPLIST_BLOCK_LEN};

use super::{SkipListBlock, SkipListFormat};

pub struct SkipListReader<R: io::Read> {
    read_count: usize,
    item_count: usize,
    current_docid: DocId,
    current_offset: usize,
    current_cursor: usize,
    skip_list_block: SkipListBlock,
    reader: R,
    skip_list_format: SkipListFormat,
}

impl<R: io::Read> SkipListReader<R> {
    pub fn open(item_count: usize, skip_list_format: SkipListFormat, reader: R) -> Self {
        Self {
            read_count: 0,
            item_count,
            current_docid: 0,
            current_offset: 0,
            current_cursor: 0,
            skip_list_block: SkipListBlock::new(&skip_list_format),
            reader,
            skip_list_format,
        }
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.item_count
    }

    // return (offset, skipped_item_count)
    pub fn seek(&mut self, query_docid: DocId) -> io::Result<(usize, usize)> {
        if self.eof() {
            return Ok((self.current_offset, 0));
        }

        let mut skipped_item_count = 0;
        loop {
            if self.current_cursor >= self.skip_list_block.len {
                self.decode_one_block()?;
                if self.skip_list_block.len == 0 {
                    break;
                }
            }
            if self.current_docid + self.skip_list_block.docids[self.current_cursor] >= query_docid
            {
                break;
            }
            self.current_docid += self.skip_list_block.docids[self.current_cursor];
            self.current_offset += self.skip_list_block.offsets[self.current_cursor] as usize;
            self.current_cursor += 1;
            skipped_item_count += 1;
        }

        Ok((self.current_offset, skipped_item_count))
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
        posting_encoder.decode_u32(&mut self.reader, &mut skip_list_block.docids[0..block_len])?;
        posting_encoder.decode_u32(&mut self.reader, &mut skip_list_block.offsets[0..block_len])?;
        if self.skip_list_format.has_tflist() {
            posting_encoder.decode_u32(
                &mut self.reader,
                &mut skip_list_block.termfreqs.as_deref_mut().unwrap()[0..block_len],
            )?;
        }
        self.current_cursor = 0;
        self.read_count += block_len;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            skiplist::{
                skip_list_reader::{self, SkipListReader},
                SkipListFormat,
            },
            PostingEncoder,
        },
        DocId, SKIPLIST_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormat::builder().build();

        let docids: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| ((i + 1) * 1000 + i % 8) as DocId)
            .collect();
        let docids = &docids[..];
        let offsets: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| ((i + 1) * 100 + i % 8) as u32)
            .collect();
        let offsets = &offsets[..];
        let docids_encoded: Vec<_> = std::iter::once(docids[0])
            .chain(docids.windows(2).map(|pair| pair[1] - pair[0]))
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
            .encode_u32(&docids_encoded[0..BLOCK_LEN], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&offsets[0..BLOCK_LEN], &mut buf)
            .unwrap();

        let buf_reader = BufReader::new(buf.as_slice());
        let mut reader = SkipListReader::open(BLOCK_LEN * 2 + 3, skip_list_format, buf_reader);
        assert!(!reader.eof());
        assert_eq!(reader.item_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, 0);

        let (offset, skipped_item_count) = reader.seek(0)?;
        assert_eq!(offset, 0);
        assert_eq!(skipped_item_count, 0);

        let (offset, skipped_item_count) = reader.seek(docids[0])?;
        assert_eq!(offset, 0);
        assert_eq!(skipped_item_count, 0);

        let (offset, skipped_item_count) = reader.seek(docids[1])?;
        assert_eq!(offset, total_offsets[0]);
        assert_eq!(skipped_item_count, 1);

        let (offset, skipped_item_count) = reader.seek(docids[4])?;
        assert_eq!(offset, total_offsets[3]);
        assert_eq!(skipped_item_count, 3);

        let (offset, skipped_item_count) = reader.seek(docids.last().cloned().unwrap())?;
        assert_eq!(offset, total_offsets.last().cloned().unwrap());
        assert_eq!(skipped_item_count, 3);

        Ok(())
    }
}
