use std::io;

use crate::{DocId, END_DOCID, INVALID_DOCID};

use super::{PostingBlock, PostingRead};

pub struct PostingIterator<'a, R: PostingRead> {
    current_docid: DocId,
    current_cursor: usize,
    block: PostingBlock,
    reader: &'a mut R,
}

impl<'a, R: PostingRead> PostingIterator<'a, R> {
    pub fn new(reader: &'a mut R) -> Self {
        let block = PostingBlock::new(reader.posting_format());

        Self {
            current_docid: INVALID_DOCID,
            current_cursor: 0,
            block,
            reader,
        }
    }

    pub fn seek(&mut self, docid: DocId) -> io::Result<DocId> {
        if self.current_docid == INVALID_DOCID {
            self.reader.decode_one_block(&mut self.block)?;
            if self.block.len == 0 {
                self.current_docid = END_DOCID;
                return Ok(END_DOCID);
            }
            self.current_docid = self.block.docids[0];
        }

        let docid = if docid == INVALID_DOCID { 0 } else { docid };

        loop {
            if self.current_docid >= docid {
                return Ok(self.current_docid);
            }
            if self.current_cursor == self.block.len - 1 {
                self.reader.decode_one_block(&mut self.block)?;
                if self.block.len == 0 {
                    self.current_docid = END_DOCID;
                    return Ok(END_DOCID);
                }
                self.current_cursor = 0;
                self.current_docid = self.block.docids[0];
                continue;
            }
            self.current_cursor += 1;
            self.current_docid = self.block.docids[self.current_cursor];
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::{
        postings::{
            BuildingPostingReader, BuildingPostingWriter, PostingBlock, PostingFormat,
            PostingIterator,
        },
        DocId, TermFreq, END_DOCID, INVALID_DOCID, POSTING_BLOCK_LEN,
    };

    #[test]
    fn test_seek_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list();
        let mut posting_block = PostingBlock::new(&posting_format);

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
        let termfreqs: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| (i % 3 + 1) as TermFreq)
            .collect();
        let termfreqs = &termfreqs[..];

        for _ in 0..termfreqs[0] {
            posting_writer.add_pos(1);
        }
        posting_writer.end_doc(docids[0]);

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        let mut posting_iterator = PostingIterator::new(&mut posting_reader);

        assert_eq!(posting_iterator.seek(INVALID_DOCID)?, 0);
        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.seek(1)?, END_DOCID);

        for _ in 0..termfreqs[1] {
            posting_writer.add_pos(1);
        }
        posting_writer.end_doc(docids[1]);

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        let mut posting_iterator = PostingIterator::new(&mut posting_reader);

        assert_eq!(posting_iterator.seek(INVALID_DOCID)?, 0);
        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.seek(1)?, docids[1]);
        assert_eq!(posting_iterator.seek(docids[1] + 1)?, END_DOCID);

        for i in 2..BLOCK_LEN {
            for _ in 0..termfreqs[i] {
                posting_writer.add_pos(1);
            }
            posting_writer.end_doc(docids[i]);
        }

        // seek one by one

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        let mut posting_iterator = PostingIterator::new(&mut posting_reader);

        for &docid in &docids[..BLOCK_LEN] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        assert_eq!(posting_iterator.seek(docids[BLOCK_LEN - 1] + 1)?, END_DOCID);

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        let mut posting_iterator = PostingIterator::new(&mut posting_reader);

        // skip some items
        //
        for (i, &docid) in docids[..BLOCK_LEN].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
            }
        }

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(1);
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN]);
        }

        // seek one by one

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        let mut posting_iterator = PostingIterator::new(&mut posting_reader);

        for &docid in &docids[..BLOCK_LEN * 2 + 3] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        assert_eq!(
            posting_iterator.seek(docids[BLOCK_LEN * 2 + 3 - 1] + 1)?,
            END_DOCID
        );

        // skip some items

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        let mut posting_iterator = PostingIterator::new(&mut posting_reader);

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
            }
        }

        // skip some blocks

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        let mut posting_iterator = PostingIterator::new(&mut posting_reader);

        let docid = docids[BLOCK_LEN + 3];
        assert_eq!(posting_iterator.seek(docid)?, docid);

        Ok(())
    }
}
