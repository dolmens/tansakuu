use std::io::{self, Read, Seek};

use crate::{DocId, POSTING_BLOCK_LEN};

use super::{
    skip_list::{NoSkipList, SkipListSeek},
    PostingBlock, PostingEncoder, PostingFormat,
};

pub trait PostingRead {
    fn posting_format(&self) -> &PostingFormat;
    fn decode_one_block(&mut self, posting_block: &mut PostingBlock) -> io::Result<()>;
}

pub struct PostingReader<R: Read + Seek, S: SkipListSeek = NoSkipList> {
    // current_seek: usize,
    last_docid: DocId,
    read_count: usize,
    doc_count: usize,
    reader: R,
    skip_list_reader: S,
    posting_format: PostingFormat,
}

impl<R: Read + Seek> PostingReader<R, NoSkipList> {
    pub fn open(posting_format: PostingFormat, doc_count: usize, reader: R) -> Self {
        Self {
            // current_seek: 0,
            last_docid: 0,
            read_count: 0,
            doc_count,
            reader,
            skip_list_reader: NoSkipList,
            posting_format,
        }
    }
}

impl<R: Read + Seek, S: SkipListSeek> PostingReader<R, S> {
    pub fn open_with_skip_list(
        posting_format: PostingFormat,
        doc_count: usize,
        reader: R,
        skip_list_reader: S,
    ) -> Self {
        Self {
            // current_seek: 0,
            last_docid: 0,
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

    pub fn last_docid(&self) -> DocId {
        self.last_docid
    }

    // pub fn seek(&mut self, docid: DocId, posting_block: &mut PostingBlock) -> io::Result<bool> {
    //     if self.eof() {
    //         return Ok(false);
    //     }
    //     let (offset, last_docid, skip_count) = self.skip_list_reader.seek(docid)?;
    //     if offset > self.current_seek {
    //         self.reader.seek(SeekFrom::Start(offset as u64))?;
    //         self.current_seek = offset;
    //         self.last_docid = last_docid;
    //         self.read_count = skip_count * POSTING_BLOCK_LEN;
    //     }
    //     loop {
    //         self.decode_one_block(posting_block)?;
    //         if posting_block.len == 0 {
    //             return Ok(false);
    //         }
    //         if posting_block.last_docid() >= docid {
    //             return Ok(true);
    //         }
    //     }
    // }
}

impl<R: Read + Seek, S: SkipListSeek> PostingRead for PostingReader<R, S> {
    fn posting_format(&self) -> &PostingFormat {
        &self.posting_format
    }

    fn decode_one_block(&mut self, posting_block: &mut PostingBlock) -> io::Result<()> {
        posting_block.len = 0;
        if self.eof() {
            return Ok(());
        }
        let block_len = std::cmp::min(self.doc_count - self.read_count, POSTING_BLOCK_LEN);
        posting_block.len = block_len;
        let posting_encoder = PostingEncoder;
        posting_encoder.decode_u32(&mut self.reader, &mut posting_block.docids[0..block_len])?;
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
        posting_block.decode_docids(self.last_docid);
        self.last_docid = posting_block.last_docid();
        self.read_count += block_len;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use crate::{
        postings::{PostingBlock, PostingEncoder, PostingFormat, PostingRead, PostingReader},
        DocId, TermFreq, POSTING_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
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
            let termfreq = (i % 3 + 1) as TermFreq;
            termfreqs.push(termfreq);
        }

        let posting_encoder = PostingEncoder;
        posting_encoder
            .encode_u32(&docids_deltas[0..BLOCK_LEN], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&termfreqs[0..BLOCK_LEN], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = PostingBlock::new(&posting_format);
        let mut reader = PostingReader::open(posting_format, BLOCK_LEN * 2 + 3, buf_reader);
        assert!(!reader.eof());
        assert_eq!(reader.doc_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, 0);

        reader.decode_one_block(&mut block)?;
        assert!(!reader.eof());
        assert_eq!(reader.doc_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, BLOCK_LEN);
        assert_eq!(block.len, BLOCK_LEN);
        assert_eq!(block.docids, &docids[0..BLOCK_LEN]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        reader.decode_one_block(&mut block)?;
        assert!(!reader.eof());
        assert_eq!(reader.doc_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, BLOCK_LEN * 2);
        assert_eq!(block.len, BLOCK_LEN);
        assert_eq!(block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        reader.decode_one_block(&mut block)?;
        assert!(reader.eof());
        assert_eq!(reader.doc_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, BLOCK_LEN * 2 + 3);
        assert_eq!(block.len, 3);
        assert_eq!(
            &block.docids[0..3],
            &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        reader.decode_one_block(&mut block)?;
        assert!(reader.eof());
        assert_eq!(reader.doc_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, BLOCK_LEN * 2 + 3);
        assert_eq!(block.len, 0);

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
