use std::io::{self, Read, Seek, SeekFrom};

use crate::{DocId, POSTING_BLOCK_LEN};

use super::{skip_list::SkipListRead, PostingBlock, PostingEncoder, PostingFormat, SkipListReader};

pub trait PostingRead {
    fn decode_one_block(
        &mut self,
        docid: DocId,
        posting_block: &mut PostingBlock,
    ) -> io::Result<bool>;
}

pub struct PostingReader<R: Read + Seek, S: SkipListRead> {
    read_count: usize,
    doc_count: usize,
    reader: R,
    skip_list_reader: S,
    posting_format: PostingFormat,
}

impl<R: Read + Seek, SR: Read> PostingReader<R, SkipListReader<SR>> {
    pub fn open(
        posting_format: PostingFormat,
        doc_count: usize,
        reader: R,
        skip_list_item_count: usize,
        skip_list_reader: SR,
    ) -> Self {
        let skip_list_format = posting_format.skip_list_format().clone();
        let skip_list_reader =
            SkipListReader::open(skip_list_format, skip_list_item_count, skip_list_reader);

        Self::open_with_skip_list_reader(posting_format, doc_count, reader, skip_list_reader)
    }
}

impl<R: Read + Seek, S: SkipListRead> PostingReader<R, S> {
    pub fn open_with_skip_list_reader(
        posting_format: PostingFormat,
        doc_count: usize,
        reader: R,
        skip_list_reader: S,
    ) -> Self {
        Self {
            read_count: 0,
            doc_count,
            reader,
            skip_list_reader,
            posting_format,
        }
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.doc_count
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    pub fn read_count(&self) -> usize {
        self.read_count
    }

    pub fn posting_format(&self) -> &PostingFormat {
        &self.posting_format
    }
}

impl<R: Read + Seek, S: SkipListRead> PostingRead for PostingReader<R, S> {
    fn decode_one_block(
        &mut self,
        docid: DocId,
        posting_block: &mut PostingBlock,
    ) -> io::Result<bool> {
        if self.doc_count == 0 {
            posting_block.prev_docid = 0;
            posting_block.last_docid = 0;
            posting_block.prev_ttf = 0;
            return Ok(false);
        }

        let (skip_found, prev_key, block_last_key, start_offset, _end_offset, skipped_count) =
            self.skip_list_reader.seek(docid as u64)?;
        posting_block.prev_docid = prev_key as DocId;
        posting_block.last_docid = block_last_key as DocId;
        posting_block.prev_ttf = if self.posting_format.has_tflist() {
            self.skip_list_reader.prev_value()
        } else {
            0
        };
        self.read_count = skipped_count * POSTING_BLOCK_LEN;
        if !skip_found {
            // The last block in skip list may be not full block,
            // so read_count would be false greater than doc_count
            if self.read_count >= self.doc_count {
                self.read_count = self.doc_count;
                return Ok(false);
            }
        }
        self.reader.seek(SeekFrom::Start(start_offset))?;

        let block_len = std::cmp::min(self.doc_count - self.read_count, POSTING_BLOCK_LEN);
        self.read_count += block_len;
        posting_block.len = block_len;
        let posting_encoder = PostingEncoder;
        posting_encoder.decode_u32(&mut self.reader, &mut posting_block.docids[0..block_len])?;
        if !skip_found {
            let mut block_last_docid = posting_block.prev_docid;
            for i in 0..block_len {
                block_last_docid += posting_block.docids[i];
            }
            posting_block.last_docid = block_last_docid;
            if block_last_docid < docid {
                posting_block.prev_docid = block_last_docid;
                if self.posting_format.has_tflist() {
                    if let Some(termfreqs) = posting_block.termfreqs.as_deref_mut() {
                        posting_encoder
                            .decode_u32(&mut self.reader, &mut termfreqs[0..block_len])?;
                        let mut prev_ttf = posting_block.prev_ttf;
                        for i in 0..block_len {
                            prev_ttf += termfreqs[i] as u64;
                        }
                        posting_block.prev_ttf = prev_ttf;
                    }
                }
                return Ok(false);
            }
        }
        if self.posting_format.has_tflist() {
            if let Some(termfreqs) = posting_block.termfreqs.as_deref_mut() {
                posting_encoder.decode_u32(&mut self.reader, &mut termfreqs[0..block_len])?;
            } else {
                let mut termfreqs = [0; POSTING_BLOCK_LEN];
                posting_encoder.decode_u32(&mut self.reader, &mut termfreqs[0..block_len])?;
            }
        }
        if self.posting_format.has_fieldmask() {
            if let Some(fieldmasks) = posting_block.fieldmasks.as_deref_mut() {
                posting_encoder.decode_u8(&mut self.reader, &mut fieldmasks[0..block_len])?;
            } else {
                let mut fieldmasks = [0; POSTING_BLOCK_LEN];
                posting_encoder.decode_u8(&mut self.reader, &mut fieldmasks[0..block_len])?;
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use crate::{
        postings::{
            skip_list::MockSkipListReader, PostingBlock, PostingEncoder, PostingFormat,
            PostingRead, PostingReader,
        },
        DocId, POSTING_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let mut buf = vec![];
        let docids_deltas: Vec<_> = (0..(BLOCK_LEN - 1) as DocId).collect();
        let docids_deltas = &docids_deltas[..];
        let docids: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .collect();
        let docids = &docids[..];

        let mut termfreqs = vec![];
        for i in 0..BLOCK_LEN - 1 {
            let termfreq = (i % 3 + 1) as u32;
            termfreqs.push(termfreq);
        }
        let termfreqs = &termfreqs[..];
        let ttf: u64 = termfreqs.iter().fold(0u64, |acc, &x| acc + (x as u64));

        let posting_encoder = PostingEncoder;
        posting_encoder.encode_u32(docids_deltas, &mut buf).unwrap();
        posting_encoder.encode_u32(termfreqs, &mut buf).unwrap();

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = PostingBlock::new(&posting_format);

        let skipbuf = vec![];
        let mut reader =
            PostingReader::open(posting_format, BLOCK_LEN - 1, buf_reader, 0, &skipbuf[..]);
        assert!(!reader.eof());
        assert_eq!(reader.doc_count, BLOCK_LEN - 1);
        assert_eq!(reader.read_count, 0);

        let last_docid = docids.last().cloned().unwrap();
        assert!(reader.decode_one_block(last_docid, &mut block)?);
        assert_eq!(block.len, BLOCK_LEN - 1);
        assert_eq!(block.prev_docid, 0);
        assert_eq!(block.last_docid, docids.last().cloned().unwrap());
        assert_eq!(block.prev_ttf, 0);
        block.decode_docids(block.prev_docid);
        assert_eq!(&block.docids[0..block.len], docids);
        assert_eq!(&block.termfreqs.as_ref().unwrap()[0..block.len], termfreqs);

        assert!(reader.eof());

        assert!(!reader.decode_one_block(last_docid + 1, &mut block)?);
        assert_eq!(block.prev_docid, docids.last().cloned().unwrap());
        assert_eq!(block.last_docid, docids.last().cloned().unwrap());
        assert_eq!(block.prev_ttf, ttf);

        Ok(())
    }

    #[test]
    fn test_with_skip_list() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let mut buf = vec![];
        let docids_deltas: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as DocId).collect();
        let docids_deltas = &docids_deltas[..];
        let docids: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .collect();
        let docids = &docids[..];

        let mut termfreqs = vec![];
        for i in 0..BLOCK_LEN * 2 + 3 {
            let termfreq = (i % 3 + 1) as u32;
            termfreqs.push(termfreq);
        }
        let termfreqs = &termfreqs[..];

        let mut block_last_docids: Vec<u64> = vec![];
        let mut block_offsets = Vec::<u64>::new();
        let mut offset: usize = 0;

        let posting_encoder = PostingEncoder;

        offset += posting_encoder
            .encode_u32(&docids_deltas[0..BLOCK_LEN], &mut buf)
            .unwrap();
        offset += posting_encoder
            .encode_u32(&termfreqs[0..BLOCK_LEN], &mut buf)
            .unwrap();
        block_last_docids.push(docids[BLOCK_LEN - 1] as u64);
        block_offsets.push(offset as u64);

        offset += posting_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        offset += posting_encoder
            .encode_u32(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        block_last_docids.push(docids[BLOCK_LEN * 2 - 1] as u64);
        block_offsets.push(offset as u64);

        posting_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();

        let skip_list_reader =
            MockSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = PostingBlock::new(&posting_format);
        let mut reader = PostingReader::open_with_skip_list_reader(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(!reader.eof());
        assert_eq!(reader.doc_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, 0);

        assert!(reader.decode_one_block(0, &mut block)?);

        assert_eq!(block.prev_docid, 0);
        assert_eq!(block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.prev_ttf, 0);
        assert_eq!(block.len, BLOCK_LEN);
        block.decode_docids(block.prev_docid);
        assert_eq!(block.docids, &docids[0..BLOCK_LEN]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = block.last_docid;
        assert!(reader.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.prev_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.prev_ttf, 0);
        assert_eq!(block.len, BLOCK_LEN);
        block.decode_docids(block.prev_docid);
        assert_eq!(block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = block.last_docid;
        assert!(reader.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.prev_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        block.decode_docids(block.prev_docid);
        assert_eq!(
            &block.docids[0..3],
            &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(reader.eof());

        let block_last_docid = block.last_docid;
        assert!(!reader.decode_one_block(block_last_docid + 1, &mut block)?);
        // if eof, block's prev_docid, last_docid would be set to the end
        assert_eq!(block.prev_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);

        // skip one block

        let skip_list_reader =
            MockSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = PostingBlock::new(&posting_format);
        let mut reader = PostingReader::open_with_skip_list_reader(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(reader.decode_one_block(docids[BLOCK_LEN - 1] + 1, &mut block)?);

        assert_eq!(block.prev_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.len, BLOCK_LEN);
        block.decode_docids(block.prev_docid);
        assert_eq!(block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = block.last_docid;
        assert!(reader.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.prev_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        block.decode_docids(block.prev_docid);
        assert_eq!(
            &block.docids[0..3],
            &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(reader.eof());

        // skip two block

        let skip_list_reader =
            MockSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = PostingBlock::new(&posting_format);
        let mut reader = PostingReader::open_with_skip_list_reader(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(reader.decode_one_block(docids[BLOCK_LEN * 2 - 1] + 1, &mut block)?);

        assert_eq!(block.prev_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        block.decode_docids(block.prev_docid);
        assert_eq!(
            &block.docids[0..3],
            &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(reader.eof());

        // skip to end

        let skip_list_reader =
            MockSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = PostingBlock::new(&posting_format);
        let mut reader = PostingReader::open_with_skip_list_reader(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(!reader.decode_one_block(docids.last().cloned().unwrap() + 1, &mut block)?);
        assert_eq!(block.prev_docid, docids.last().cloned().unwrap());
        assert_eq!(block.last_docid, docids.last().cloned().unwrap());

        assert!(reader.eof());

        Ok(())
    }

    #[test]
    fn test_last_block_is_not_full() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let mut buf = vec![];
        let docids_deltas: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as DocId).collect();
        let docids_deltas = &docids_deltas[..];
        let docids: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .collect();
        let docids = &docids[..];

        let mut termfreqs = vec![];
        for i in 0..BLOCK_LEN * 2 + 3 {
            let termfreq = (i % 3 + 1) as u32;
            termfreqs.push(termfreq);
        }
        let termfreqs = &termfreqs[..];

        let mut block_last_docids: Vec<u64> = vec![];
        let mut block_offsets = Vec::<u64>::new();
        let mut offset: usize = 0;

        let posting_encoder = PostingEncoder;

        offset += posting_encoder
            .encode_u32(&docids_deltas[0..BLOCK_LEN], &mut buf)
            .unwrap();
        offset += posting_encoder
            .encode_u32(&termfreqs[0..BLOCK_LEN], &mut buf)
            .unwrap();
        block_last_docids.push(docids[BLOCK_LEN - 1] as u64);
        block_offsets.push(offset as u64);

        offset += posting_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        offset += posting_encoder
            .encode_u32(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        block_last_docids.push(docids[BLOCK_LEN * 2 - 1] as u64);
        block_offsets.push(offset as u64);

        offset += posting_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        offset += posting_encoder
            .encode_u32(&termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        block_last_docids.push(docids[BLOCK_LEN * 2 + 3 - 1] as u64);
        block_offsets.push(offset as u64);

        let skip_list_reader =
            MockSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = PostingBlock::new(&posting_format);
        let mut reader = PostingReader::open_with_skip_list_reader(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(!reader.eof());

        assert!(!reader.decode_one_block(docids.last().cloned().unwrap() + 1, &mut block)?);
        assert_eq!(block.prev_docid, docids.last().cloned().unwrap());
        assert_eq!(block.last_docid, docids.last().cloned().unwrap());

        assert!(reader.eof());

        Ok(())
    }

    // #[test]
    // fn test_block_with_skip_list() -> io::Result<()> {
    //     const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
    //     let block_count = 5;
    //     assert!(block_count >= 4);
    //     let docids: Vec<_> = (0..BLOCK_LEN * block_count + 3)
    //         .enumerate()
    //         .map(|(i, _)| (i * 10) as DocId)
    //         .collect();
    //     let docids_encoded: Vec<_> = std::iter::once(docids[0])
    //         .chain(docids.windows(2).map(|pair| pair[1] - pair[0]))
    //         .collect();

    //     let mut buf = vec![];
    //     let mut skip_buf = vec![];

    //     let mut end_docids = vec![];
    //     let mut flushed_sizes = vec![];

    //     let posting_encoder = PostingEncoder;

    //     for i in 0..block_count {
    //         end_docids.push(docids[(i + 1) * BLOCK_LEN - 1]);
    //         let flushed_size = posting_encoder
    //             .encode_u32(
    //                 &docids_encoded[i * BLOCK_LEN..(i + 1) * BLOCK_LEN],
    //                 &mut buf,
    //             )
    //             .unwrap();
    //         flushed_sizes.push(flushed_size as u32);
    //     }

    //     posting_encoder
    //         .encode_u32(
    //             &docids_encoded[BLOCK_LEN * block_count..BLOCK_LEN * block_count + 3],
    //             &mut buf,
    //         )
    //         .unwrap();

    //     let end_docids_encoded: Vec<_> = std::iter::once(end_docids[0])
    //         .chain(end_docids.windows(2).map(|pair| pair[1] - pair[0]))
    //         .collect();

    //     posting_encoder.encode_u32(&end_docids_encoded[..], &mut skip_buf)?;
    //     posting_encoder.encode_u32(&flushed_sizes[..], &mut skip_buf)?;

    //     let posting_format = PostingFormat::builder().build();
    //     let mut block = PostingBlock::new(&posting_format);

    //     let skip_list_format = posting_format.skip_list_format().clone();

    //     let skip_buf_reader = BufReader::new(skip_buf.as_slice());
    //     let skip_list_reader =
    //         SkipListReader::open(skip_list_format.clone(), block_count, skip_buf_reader);

    //     let buf_reader = Cursor::new(buf.as_slice());
    //     let mut reader = PostingReader::open_with_skip_list(
    //         posting_format.clone(),
    //         BLOCK_LEN * block_count + 3,
    //         buf_reader,
    //         skip_list_reader,
    //     );

    //     for i in 0..block_count {
    //         let block_last_id = end_docids[i];
    //         assert!(reader.seek(block_last_id, &mut block)?);
    //         assert_eq!(block.len, BLOCK_LEN);
    //         assert_eq!(block.docids, &docids[i * BLOCK_LEN..(i + 1) * BLOCK_LEN]);
    //     }

    //     assert!(reader.seek(end_docids[block_count - 1] + 1, &mut block)?);
    //     assert_eq!(block.len, 3);
    //     assert_eq!(
    //         &block.docids[0..3],
    //         &docids[BLOCK_LEN * block_count..BLOCK_LEN * block_count + 3]
    //     );

    //     let skip_buf_reader = BufReader::new(skip_buf.as_slice());
    //     let skip_list_reader =
    //         SkipListReader::open(skip_list_format.clone(), block_count, skip_buf_reader);

    //     let buf_reader = Cursor::new(buf.as_slice());
    //     let mut reader = PostingReader::open_with_skip_list(
    //         posting_format.clone(),
    //         BLOCK_LEN * block_count + 3,
    //         buf_reader,
    //         skip_list_reader,
    //     );

    //     assert!(reader.seek(((3 * BLOCK_LEN - 1) * 10) as DocId, &mut block)?);
    //     assert_eq!(block.len, BLOCK_LEN);
    //     assert_eq!(block.docids, &docids[2 * BLOCK_LEN..(2 + 1) * BLOCK_LEN]);

    //     assert!(reader.seek((block_count * BLOCK_LEN * 10) as DocId, &mut block)?);
    //     assert_eq!(block.len, 3);
    //     assert_eq!(
    //         &block.docids[0..3],
    //         &docids[BLOCK_LEN * block_count..BLOCK_LEN * block_count + 3]
    //     );

    //     let skip_buf_reader = BufReader::new(skip_buf.as_slice());
    //     let skip_list_reader =
    //         SkipListReader::open(skip_list_format.clone(), block_count, skip_buf_reader);

    //     let buf_reader = Cursor::new(buf.as_slice());
    //     let mut reader = PostingReader::open_with_skip_list(
    //         posting_format.clone(),
    //         BLOCK_LEN * block_count + 3,
    //         buf_reader,
    //         skip_list_reader,
    //     );

    //     assert!(reader.seek((block_count * BLOCK_LEN * 10) as DocId, &mut block)?);
    //     assert_eq!(block.len, 3);
    //     assert_eq!(
    //         &block.docids[0..3],
    //         &docids[BLOCK_LEN * block_count..BLOCK_LEN * block_count + 3]
    //     );

    //     Ok(())
    // }
}
