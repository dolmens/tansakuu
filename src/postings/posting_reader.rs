use std::io;

use crate::DocId;

use super::{
    positions::{PositionListBlock, PositionListDecode},
    DocListBlock, DocListDecode, PostingFormat,
};

pub trait PostingRead {
    fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool>;

    fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool>;
}

pub struct PostingReader<D: DocListDecode, P: PositionListDecode> {
    doc_list_decoder: D,
    position_list_decoder: Option<P>,
    posting_format: PostingFormat,
}

impl<D: DocListDecode, P: PositionListDecode> PostingReader<D, P> {
    pub fn new(
        posting_format: PostingFormat,
        doc_list_decoder: D,
        position_list_decoder: Option<P>,
    ) -> Self {
        Self {
            doc_list_decoder,
            position_list_decoder,
            posting_format,
        }
    }

    pub fn doc_list_decoder(&self) -> &D {
        &self.doc_list_decoder
    }

    pub fn position_list_decoder(&self) -> Option<&P> {
        self.position_list_decoder.as_ref()
    }

    pub fn posting_format(&self) -> &PostingFormat {
        &self.posting_format
    }
}

impl<D: DocListDecode, P: PositionListDecode> PostingRead for PostingReader<D, P> {
    fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.doc_list_decoder
            .decode_one_block(docid, doc_list_block)
    }

    fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if let Some(position_list_reader) = self.position_list_decoder.as_mut() {
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
            compression::BlockEncoder, doc_list_decoder::DocListDecoder,
            positions::none_position_list_decoder, skip_list::BasicSkipListReader, DocListBlock,
            PostingFormat, PostingRead, PostingReader,
        },
        DocId, DOC_LIST_BLOCK_LEN,
    };

    #[test]
    fn test_with_skip_list() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
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

        let block_encoder = BlockEncoder;

        offset += block_encoder
            .encode_u32(&docids_deltas[0..BLOCK_LEN], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[0..BLOCK_LEN], &mut buf)
            .unwrap();
        block_last_docids.push(docids[BLOCK_LEN - 1] as u64);
        block_offsets.push(offset as u64);

        offset += block_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)
            .unwrap();
        block_last_docids.push(docids[BLOCK_LEN * 2 - 1] as u64);
        block_offsets.push(offset as u64);

        offset += block_encoder
            .encode_u32(&docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        offset += block_encoder
            .encode_u32(&termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)
            .unwrap();
        block_last_docids.push(docids[BLOCK_LEN * 2 + 3 - 1] as u64);
        block_offsets.push(offset as u64);

        let posting_format = PostingFormat::builder().with_tflist().build();
        let doc_list_format = posting_format.doc_list_format().clone();

        let buf_reader = Cursor::new(buf.as_slice());

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let doc_list_decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format.clone(),
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        let mut block = DocListBlock::new(&doc_list_format);
        let mut reader = PostingReader::new(
            posting_format,
            doc_list_decoder,
            none_position_list_decoder(),
        );

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

        let block_last_docid = block.last_docid;
        assert!(!reader.decode_one_block(block_last_docid + 1, &mut block)?);

        // skip one block

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = DocListBlock::new(&doc_list_format);
        let doc_list_decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format.clone(),
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );
        let mut reader = PostingReader::new(
            posting_format,
            doc_list_decoder,
            none_position_list_decoder(),
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

        // skip two block

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = DocListBlock::new(&doc_list_format);
        let doc_list_decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format.clone(),
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );
        let mut reader = PostingReader::new(
            posting_format,
            doc_list_decoder,
            none_position_list_decoder(),
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

        // skip to end

        let skip_list_reader =
            BasicSkipListReader::new(block_last_docids.clone(), block_offsets.clone(), None);

        let buf_reader = Cursor::new(buf.as_slice());
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut block = DocListBlock::new(&doc_list_format);
        let doc_list_decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format,
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );
        let mut reader = PostingReader::new(
            posting_format,
            doc_list_decoder,
            none_position_list_decoder(),
        );

        assert!(!reader.decode_one_block(docids.last().cloned().unwrap() + 1, &mut block)?);

        Ok(())
    }
}
