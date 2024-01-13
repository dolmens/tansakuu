use std::io::{self, Read};

use crate::{DocId, POSTING_BLOCK_LEN};

use super::{
    skiplist::{NoSkipList, SkipListSeek},
    PostingBlock, PostingEncoder, PostingFormat,
};

pub struct PostingReader<R: Read, S: SkipListSeek = NoSkipList> {
    last_docid: DocId,
    read_count: usize,
    doc_count: usize,
    reader: R,
    skip_list_reader: S,
    posting_format: PostingFormat,
}

impl<R: Read> PostingReader<R, NoSkipList> {
    pub fn open(posting_format: PostingFormat, doc_count: usize, reader: R) -> Self {
        Self {
            last_docid: 0,
            read_count: 0,
            doc_count,
            reader,
            skip_list_reader: NoSkipList,
            posting_format,
        }
    }
}

impl<R: Read, S: SkipListSeek> PostingReader<R, S> {
    pub fn open_with_skip_list(
        posting_format: PostingFormat,
        doc_count: usize,
        reader: R,
        skip_list_reader: S,
    ) -> Self {
        Self {
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

    pub fn seek_block(
        &mut self,
        docid: DocId,
        posting_block: &mut PostingBlock,
    ) -> io::Result<bool> {
        let (offset, skipped_item_count) = self.skip_list_reader.seek(docid)?;
        if offset > 0 {}

        Ok(false)
    }

    pub fn decode_one_block(&mut self, posting_block: &mut PostingBlock) -> io::Result<()> {
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
        posting_block.decode(self.last_docid);
        self.last_docid = posting_block.last_docid();
        self.read_count += block_len;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use posting_block::PostingBlock;

    use crate::{
        postings::{posting_block, PostingEncoder, PostingFormat, PostingReader},
        DocId, TermFreq, POSTING_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let mut buf = vec![];
        let mut docids = vec![];
        let mut termfreqs = vec![];
        for i in 0..BLOCK_LEN * 2 + 3 {
            let docid = (i * 5 + i % 3) as DocId;
            let termfreq = (i % 3 + 1) as TermFreq;
            docids.push(docid);
            termfreqs.push(termfreq);
        }
        let docids_encoded: Vec<_> = std::iter::once(docids[0])
            .chain(docids.windows(2).map(|pair| pair[1] - pair[0]))
            .collect();

        let posting_encoder = PostingEncoder;
        posting_encoder
            .encode_u32(&docids_encoded[0..BLOCK_LEN], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&termfreqs[0..BLOCK_LEN], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&docids_encoded[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&docids_encoded[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        posting_encoder
            .encode_u32(&termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();

        let buf_reader = BufReader::new(buf.as_slice());
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
}
