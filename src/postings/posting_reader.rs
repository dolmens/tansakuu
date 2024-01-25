use std::io::{self, Read, Seek, SeekFrom};

use crate::{DocId, POSTING_BLOCK_LEN};

use super::{
    positions::{
        none_position_list_reader, EmptyPositionListReader, PositionListBlock, PositionListRead,
    },
    skip_list::{empty_skip_list_reader, EmptySkipListReader, SkipListRead, SkipListReader},
    PostingBlock, PostingEncoder, PostingFormat,
};

pub trait PostingRead {
    fn decode_one_block(
        &mut self,
        docid: DocId,
        posting_block: &mut PostingBlock,
    ) -> io::Result<bool>;

    fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool>;
}

pub struct PostingReader<R: Read + Seek, S: SkipListRead, P: PositionListRead> {
    read_count: usize,
    doc_count: usize,
    input_reader: R,
    skip_list_reader: S,
    position_list_reader: Option<P>,
    posting_format: PostingFormat,
}

pub struct PostingReaderBuilder<R: Read + Seek, S: SkipListRead, P: PositionListRead> {
    doc_count: usize,
    input_reader: R,
    skip_list_reader: S,
    position_list_reader: Option<P>,
    posting_format: PostingFormat,
}

impl<R: Read + Seek> PostingReaderBuilder<R, EmptySkipListReader, EmptyPositionListReader> {
    pub fn new(posting_format: PostingFormat, doc_count: usize, input_reader: R) -> Self {
        Self {
            doc_count,
            input_reader,
            skip_list_reader: empty_skip_list_reader(),
            position_list_reader: none_position_list_reader(),
            posting_format,
        }
    }
}

impl<R: Read + Seek, S: SkipListRead, P: PositionListRead> PostingReaderBuilder<R, S, P> {
    pub fn with_skip_list_input_reader<SR: Read>(
        self,
        skip_list_item_count: usize,
        skip_list_input_reader: SR,
    ) -> PostingReaderBuilder<R, SkipListReader<SR>, P> {
        let skip_list_format = self.posting_format.skip_list_format().clone();
        let skip_list_reader = SkipListReader::open(
            skip_list_format,
            skip_list_item_count,
            skip_list_input_reader,
        );
        PostingReaderBuilder {
            doc_count: self.doc_count,
            input_reader: self.input_reader,
            skip_list_reader,
            position_list_reader: self.position_list_reader,
            posting_format: self.posting_format,
        }
    }

    pub fn with_skip_list_reader<SR: SkipListRead>(
        self,
        skip_list_reader: SR,
    ) -> PostingReaderBuilder<R, SR, P> {
        PostingReaderBuilder {
            doc_count: self.doc_count,
            input_reader: self.input_reader,
            skip_list_reader,
            position_list_reader: self.position_list_reader,
            posting_format: self.posting_format,
        }
    }

    pub fn with_position_list_reader<PR: PositionListRead>(
        self,
        position_list_reader: Option<PR>,
    ) -> PostingReaderBuilder<R, S, PR> {
        PostingReaderBuilder {
            doc_count: self.doc_count,
            input_reader: self.input_reader,
            skip_list_reader: self.skip_list_reader,
            position_list_reader,
            posting_format: self.posting_format,
        }
    }
}

impl<R: Read + Seek, S: SkipListRead, P: PositionListRead> PostingReader<R, S, P> {
    pub fn open(
        posting_format: PostingFormat,
        doc_count: usize,
        input_reader: R,
        skip_list_reader: S,
        position_list_reader: Option<P>,
    ) -> Self {
        Self {
            read_count: 0,
            doc_count,
            input_reader,
            skip_list_reader,
            position_list_reader,
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
        self.skip_list_reader.current_key() as DocId
    }

    pub fn last_ttf(&self) -> u64 {
        self.skip_list_reader.block_last_value()
    }

    pub fn posting_format(&self) -> &PostingFormat {
        &self.posting_format
    }
}

impl<R: Read + Seek, S: SkipListRead, P: PositionListRead> PostingRead for PostingReader<R, S, P> {
    fn decode_one_block(
        &mut self,
        docid: DocId,
        posting_block: &mut PostingBlock,
    ) -> io::Result<bool> {
        let (skip_found, prev_key, block_last_key, start_offset, _end_offset, skipped_count) =
            self.skip_list_reader.seek(docid as u64)?;
        if !skip_found {
            self.read_count = self.doc_count;
            return Ok(false);
        }

        // Only the last block allowed to be not full
        self.read_count = skipped_count * POSTING_BLOCK_LEN;
        posting_block.base_docid = prev_key as DocId;
        posting_block.last_docid = block_last_key as DocId;
        if self.posting_format.has_tflist() {
            posting_block.base_ttf = self.skip_list_reader.prev_value();
        };

        self.input_reader.seek(SeekFrom::Start(start_offset))?;

        let block_len = std::cmp::min(self.doc_count - self.read_count, POSTING_BLOCK_LEN);
        self.read_count += block_len;
        posting_block.len = block_len;
        let posting_encoder = PostingEncoder;
        posting_encoder.decode_u32(
            &mut self.input_reader,
            &mut posting_block.docids[0..block_len],
        )?;
        if self.posting_format.has_tflist() {
            if let Some(termfreqs) = posting_block.termfreqs.as_deref_mut() {
                posting_encoder.decode_u32(&mut self.input_reader, &mut termfreqs[0..block_len])?;
            } else {
                let mut termfreqs = [0; POSTING_BLOCK_LEN];
                posting_encoder.decode_u32(&mut self.input_reader, &mut termfreqs[0..block_len])?;
            }
        }
        if self.posting_format.has_fieldmask() {
            if let Some(fieldmasks) = posting_block.fieldmasks.as_deref_mut() {
                posting_encoder.decode_u8(&mut self.input_reader, &mut fieldmasks[0..block_len])?;
            } else {
                let mut fieldmasks = [0; POSTING_BLOCK_LEN];
                posting_encoder.decode_u8(&mut self.input_reader, &mut fieldmasks[0..block_len])?;
            }
        }

        Ok(true)
    }

    fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if let Some(position_list_reader) = self.position_list_reader.as_mut() {
            position_list_reader.decode_one_block(from_ttf, position_list_block)
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
            positions::none_position_list_reader, skip_list::MockSkipListReader, PostingBlock,
            PostingEncoder, PostingFormat, PostingRead, PostingReader,
        },
        DocId, POSTING_BLOCK_LEN,
    };

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
        let mut reader = PostingReader::open(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
            none_position_list_reader(),
        );

        assert!(!reader.eof());
        assert_eq!(reader.doc_count, BLOCK_LEN * 2 + 3);
        assert_eq!(reader.read_count, 0);

        assert!(reader.decode_one_block(0, &mut block)?);

        assert_eq!(block.base_docid, 0);
        assert_eq!(block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.base_ttf, 0);
        assert_eq!(block.len, BLOCK_LEN);
        block.decode_docids(block.base_docid);
        assert_eq!(block.docids, &docids[0..BLOCK_LEN]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = block.last_docid;
        assert!(reader.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.base_ttf, 0);
        assert_eq!(block.len, BLOCK_LEN);
        block.decode_docids(block.base_docid);
        assert_eq!(block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = block.last_docid;
        assert!(reader.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        block.decode_docids(block.base_docid);
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

        // skip one block

        let skip_list_reader =
            MockSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = PostingBlock::new(&posting_format);
        let mut reader = PostingReader::open(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
            none_position_list_reader(),
        );

        assert!(reader.decode_one_block(docids[BLOCK_LEN - 1] + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.len, BLOCK_LEN);
        block.decode_docids(block.base_docid);
        assert_eq!(block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
        assert_eq!(
            &block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = block.last_docid;
        assert!(reader.decode_one_block(block_last_docid + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        block.decode_docids(block.base_docid);
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
        let mut reader = PostingReader::open(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
            none_position_list_reader(),
        );

        assert!(reader.decode_one_block(docids[BLOCK_LEN * 2 - 1] + 1, &mut block)?);

        assert_eq!(block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(block.last_docid, docids[BLOCK_LEN * 2 + 2]);
        assert_eq!(block.len, 3);
        block.decode_docids(block.base_docid);
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
        let mut reader = PostingReader::open(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
            none_position_list_reader(),
        );

        assert!(!reader.decode_one_block(docids.last().cloned().unwrap() + 1, &mut block)?);

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
        let mut reader = PostingReader::open(
            posting_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
            none_position_list_reader(),
        );

        assert!(!reader.eof());
        assert!(!reader.decode_one_block(docids.last().cloned().unwrap() + 1, &mut block)?);
        assert!(reader.eof());

        Ok(())
    }
}
