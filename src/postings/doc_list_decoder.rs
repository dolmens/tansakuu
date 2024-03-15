use std::io::{self, Read, Seek, SeekFrom};

use crate::{DocId, DocId32, DOC_LIST_BLOCK_LEN, MAX_UNCOMPRESSED_DOC_LIST_LEN};

use super::{
    compression::BlockEncoder,
    skip_list::{SkipListRead, SkipListReader},
    DocListBlock, DocListFormat,
};

pub trait DocListDecode {
    fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool>;

    fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool>;

    fn decode_fieldmask_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool>;

    fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        if !self.decode_doc_buffer(docid, doc_list_block)? {
            return Ok(false);
        }
        self.decode_tf_buffer(doc_list_block)?;
        self.decode_fieldmask_buffer(doc_list_block)?;

        Ok(true)
    }
}

pub struct DocListDecoder<R: Read + Seek, S: SkipListRead> {
    read_count: usize,
    df: usize,
    reader: R,
    skip_list_reader: Option<S>,
    doc_list_format: DocListFormat,
}

impl<R: Read + Seek, S: Read> DocListDecoder<R, SkipListReader<S>> {
    pub fn open(
        doc_list_format: DocListFormat,
        df: usize,
        reader: R,
        skip_list_input_reader: S,
    ) -> Self {
        let skip_list_reader = if df > MAX_UNCOMPRESSED_DOC_LIST_LEN {
            let skip_list_format = doc_list_format.skip_list_format().clone();
            let skip_list_item_count = (df + DOC_LIST_BLOCK_LEN - 1) / DOC_LIST_BLOCK_LEN;
            Some(SkipListReader::open(
                skip_list_format,
                skip_list_item_count,
                skip_list_input_reader,
            ))
        } else {
            None
        };

        Self {
            read_count: 0,
            df,
            reader,
            skip_list_reader,
            doc_list_format,
        }
    }
}

impl<R: Read + Seek, S: SkipListRead> DocListDecoder<R, S> {
    pub fn open_with_skip_list_reader(
        doc_list_format: DocListFormat,
        doc_count: usize,
        reader: R,
        skip_list_reader: S,
    ) -> Self {
        Self {
            read_count: 0,
            df: doc_count,
            reader,
            skip_list_reader: Some(skip_list_reader),
            doc_list_format,
        }
    }

    pub fn open_with_skip_list_reader_optional(
        doc_list_format: DocListFormat,
        doc_count: usize,
        reader: R,
        skip_list_reader: Option<S>,
    ) -> Self {
        Self {
            read_count: 0,
            df: doc_count,
            reader,
            skip_list_reader,
            doc_list_format,
        }
    }

    fn decode_doc_buffer_short_list(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        if self.read_count == self.df {
            return Ok(false);
        }

        let block_len = self.df;
        self.read_count += block_len;
        doc_list_block.len = block_len;
        let block_encoder = BlockEncoder;
        block_encoder.decode_u32(&mut self.reader, &mut doc_list_block.docids[0..block_len])?;

        doc_list_block.base_docid = 0;
        let last_docid = doc_list_block.docids[0..block_len].iter().sum::<DocId32>() as DocId;
        if last_docid < docid {
            return Ok(false);
        }
        doc_list_block.last_docid = last_docid;

        Ok(true)
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.df
    }

    pub fn df(&self) -> usize {
        self.df
    }

    pub fn skip_list_reader(&self) -> Option<&S> {
        self.skip_list_reader.as_ref()
    }

    pub fn doc_list_format(&self) -> &DocListFormat {
        &self.doc_list_format
    }
}

impl<R: Read + Seek, S: SkipListRead> DocListDecode for DocListDecoder<R, S> {
    fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        if self.df <= MAX_UNCOMPRESSED_DOC_LIST_LEN {
            return self.decode_doc_buffer_short_list(docid, doc_list_block);
        }

        let skip_list_reader = self.skip_list_reader.as_mut().unwrap();
        let (skip_found, prev_key, block_last_key, start_offset, _end_offset, skipped_count) =
            skip_list_reader.seek(docid as u64)?;
        if !skip_found {
            self.read_count = self.df;
            return Ok(false);
        }

        // Only the last block allowed to be not full
        self.read_count = skipped_count * DOC_LIST_BLOCK_LEN;
        doc_list_block.base_docid = prev_key as DocId;
        doc_list_block.last_docid = block_last_key as DocId;
        if self.doc_list_format.has_tflist() {
            doc_list_block.base_ttf = skip_list_reader.prev_value();
        };

        self.reader.seek(SeekFrom::Start(start_offset))?;

        let block_len = std::cmp::min(self.df - self.read_count, DOC_LIST_BLOCK_LEN);
        self.read_count += block_len;
        doc_list_block.len = block_len;
        let block_encoder = BlockEncoder;
        block_encoder.decode_u32(&mut self.reader, &mut doc_list_block.docids[0..block_len])?;

        Ok(true)
    }

    fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        if self.doc_list_format.has_tflist() {
            let termfreqs = doc_list_block.termfreqs.as_deref_mut().unwrap();
            let block_encoder = BlockEncoder;
            block_encoder.decode_u32(&mut self.reader, &mut termfreqs[0..doc_list_block.len])?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn decode_fieldmask_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        if self.doc_list_format.has_fieldmask() {
            let fieldmasks = doc_list_block.fieldmasks.as_deref_mut().unwrap();
            let block_encoder = BlockEncoder;
            block_encoder.decode_u8(&mut self.reader, &mut fieldmasks[0..doc_list_block.len])?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use crate::{
        postings::{
            compression::BlockEncoder,
            doc_list_decoder::{DocListDecode, DocListDecoder},
            skip_list::BasicSkipListReader,
            DocListBlock, DocListFormat,
        },
        DocId, DocId32, DOC_LIST_BLOCK_LEN,
    };

    #[test]
    fn test_with_skip_list() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let mut buf = vec![];
        let docids_deltas: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as DocId32).collect();
        let docids_deltas = &docids_deltas[..];
        let docid32s: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .collect();
        let docid32s = &docid32s[..];
        let docids: Vec<_> = docid32s.iter().map(|&docid| docid as DocId).collect();
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

        let block_encoder = BlockEncoder;

        offset += block_encoder
            .encode_u32(&docids_deltas[0..BLOCK_LEN], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[0..BLOCK_LEN], &mut buf)
            .unwrap();
        block_last_docids.push(docid32s[BLOCK_LEN - 1] as u64);
        block_offsets.push(offset as u64);

        offset += block_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        block_last_docids.push(docid32s[BLOCK_LEN * 2 - 1] as u64);
        block_offsets.push(offset as u64);

        offset += block_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        block_last_docids.push(docid32s[BLOCK_LEN * 2 + 3 - 1] as u64);
        block_offsets.push(offset as u64);

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let doc_list_format = DocListFormat::builder().with_tflist().build();
        let mut block = DocListBlock::new(&doc_list_format);
        let mut decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(!decoder.eof());
        assert_eq!(decoder.df, BLOCK_LEN * 2 + 3);
        assert_eq!(decoder.read_count, 0);

        assert!(decoder.decode_one_block(0, &mut block)?);

        assert_eq!(block.base_docid, 0);
        assert_eq!(block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.base_ttf, 0);
        assert_eq!(block.len, BLOCK_LEN);
        let gotids = block.decode_docids(block.base_docid);
        assert_eq!(gotids, &docids[0..BLOCK_LEN]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = block.last_docid;
        assert!(decoder.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.base_ttf, 0);
        assert_eq!(block.len, BLOCK_LEN);
        let gotids = block.decode_docids(block.base_docid);
        assert_eq!(gotids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = block.last_docid;
        assert!(decoder.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        let gotids = block.decode_docids(block.base_docid);
        assert_eq!(&gotids[0..3], &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(decoder.eof());

        let block_last_docid = block.last_docid;
        assert!(!decoder.decode_one_block(block_last_docid + 1, &mut block)?);

        // skip one block

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let doc_list_format = DocListFormat::builder().with_tflist().build();
        let mut block = DocListBlock::new(&doc_list_format);
        let mut decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(decoder.decode_one_block(docids[BLOCK_LEN - 1] + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.len, BLOCK_LEN);
        let gotids = block.decode_docids(block.base_docid);
        assert_eq!(gotids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = block.last_docid;
        assert!(decoder.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        let gotids = block.decode_docids(block.base_docid);
        assert_eq!(&gotids[0..3], &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(decoder.eof());

        // skip two block

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let doc_list_format = DocListFormat::builder().with_tflist().build();
        let mut block = DocListBlock::new(&doc_list_format);
        let mut decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(decoder.decode_one_block(docids[BLOCK_LEN * 2 - 1] + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        let gotids = block.decode_docids(block.base_docid);
        assert_eq!(&gotids[0..3], &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(decoder.eof());

        // skip to end

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let doc_list_format = DocListFormat::builder().with_tflist().build();
        let mut block = DocListBlock::new(&doc_list_format);
        let mut decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(!decoder.decode_one_block(docids.last().cloned().unwrap() + 1, &mut block)?);

        assert!(decoder.eof());

        Ok(())
    }

    #[test]
    fn test_last_block_is_not_full() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let mut buf = vec![];
        let docids_deltas: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as DocId32).collect();
        let docids_deltas = &docids_deltas[..];
        let docid32s: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .collect();
        let docid32s = &docid32s[..];
        let docids: Vec<_> = docid32s.iter().map(|&docid| docid as DocId).collect();
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

        let block_encoder = BlockEncoder;

        offset += block_encoder
            .encode_u32(&docids_deltas[0..BLOCK_LEN], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[0..BLOCK_LEN], &mut buf)
            .unwrap();
        block_last_docids.push(docid32s[BLOCK_LEN - 1] as u64);
        block_offsets.push(offset as u64);

        offset += block_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        block_last_docids.push(docid32s[BLOCK_LEN * 2 - 1] as u64);
        block_offsets.push(offset as u64);

        offset += block_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        block_last_docids.push(docid32s[BLOCK_LEN * 2 + 3 - 1] as u64);
        block_offsets.push(offset as u64);

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let doc_list_format = DocListFormat::builder().with_tflist().build();
        let mut block = DocListBlock::new(&doc_list_format);
        let mut decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(!decoder.eof());
        assert!(!decoder.decode_one_block(docids.last().cloned().unwrap() + 1, &mut block)?);
        assert!(decoder.eof());

        Ok(())
    }
}
