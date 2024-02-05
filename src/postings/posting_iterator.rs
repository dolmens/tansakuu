use std::io;

use crate::{DocId, END_DOCID, END_POSITION, INVALID_DOCID};

use super::{
    positions::PositionListBlock, BuildingPostingList, BuildingPostingReader, DocListBlock,
    PostingFormat, PostingRead,
};

pub struct PostingIterator<R: PostingRead> {
    current_docid: DocId,
    current_ttf: u64,
    current_tf: u32,
    current_fieldmask: u8,
    block_cursor: usize,
    doc_list_block: DocListBlock,
    position_docid: DocId,
    current_position: u32,
    current_position_index: u32,
    position_block_cursor: usize,
    position_list_block: Option<Box<PositionListBlock>>,
    posting_reader: R,
    // doc_list_decoder: D,
    // position_list_decoder: Option<P>,
    posting_format: PostingFormat,
}

impl<'a> PostingIterator<BuildingPostingReader<'a>> {
    pub fn open_building_posting_list(building_posting_list: &'a BuildingPostingList) -> Self {
        let posting_format = building_posting_list.posting_format.clone();
        let posting_reader = BuildingPostingReader::open(building_posting_list);
        Self::new(posting_format, posting_reader)
    }
}

impl<R: PostingRead> PostingIterator<R> {
    pub fn new(posting_format: PostingFormat, posting_reader: R) -> Self {
        let doc_list_block = DocListBlock::new(posting_format.doc_list_format());
        let position_list_block =
            if posting_format.has_tflist() && posting_format.has_position_list() {
                Some(Box::new(PositionListBlock::new()))
            } else {
                None
            };

        Self {
            current_docid: 0,
            current_ttf: 0,
            current_tf: 0,
            current_fieldmask: 0,
            block_cursor: 0,
            doc_list_block,
            position_docid: INVALID_DOCID,
            current_position: 0,
            current_position_index: 0,
            position_block_cursor: 0,
            position_list_block,
            posting_reader,
            posting_format,
        }
    }

    pub fn seek(&mut self, docid: DocId) -> io::Result<DocId> {
        let docid = std::cmp::max(self.current_docid, docid);

        if self.block_cursor == self.doc_list_block.len || docid > self.doc_list_block.last_docid {
            if !self
                .posting_reader
                .decode_one_block(docid, &mut self.doc_list_block)?
            {
                self.current_docid = END_DOCID;
                return Ok(END_DOCID);
            }
            self.current_docid = self.doc_list_block.base_docid + self.doc_list_block.docids[0];
            if let Some(termfreqs) = &self.doc_list_block.termfreqs {
                self.current_ttf = self.doc_list_block.base_ttf;
                self.current_tf = termfreqs[0];
            }
            if let Some(fieldmasks) = &self.doc_list_block.fieldmasks {
                self.current_fieldmask = fieldmasks[0];
            }
            self.block_cursor = 1;
        }

        while self.current_docid < docid {
            self.current_docid += self.doc_list_block.docids[self.block_cursor];
            if let Some(termfreqs) = &self.doc_list_block.termfreqs {
                self.current_ttf += self.current_tf as u64;
                self.current_tf = termfreqs[self.block_cursor];
            }
            if let Some(fieldmasks) = &self.doc_list_block.fieldmasks {
                self.current_fieldmask = fieldmasks[self.block_cursor];
            }
            self.block_cursor += 1;
        }

        Ok(self.current_docid)
    }

    pub fn seek_pos(&mut self, pos: u32) -> io::Result<u32> {
        if !self.posting_format.has_tflist() || !self.posting_format.has_position_list() {
            return Ok(END_POSITION);
        }
        let pos = std::cmp::max(self.current_position, pos);
        let position_list_block = self.position_list_block.as_deref_mut().unwrap();

        if self.position_docid != self.current_docid {
            if self.position_block_cursor == position_list_block.len
                || self.current_ttf
                    > position_list_block.start_ttf + (position_list_block.len as u64)
            {
                if !self
                    .posting_reader
                    .decode_one_position_block(self.current_ttf, position_list_block)?
                {
                    return Ok(END_POSITION);
                }
            }
            self.position_docid = self.current_docid;
            self.position_block_cursor =
                (self.current_ttf - position_list_block.start_ttf) as usize;
            self.current_position = position_list_block.positions[self.position_block_cursor];
            self.current_position_index = 0;
            self.position_block_cursor += 1;
        }

        while self.current_position < pos {
            self.current_position_index += 1;
            if self.current_position_index == self.current_tf {
                return Ok(END_POSITION);
            }
            if self.position_block_cursor == position_list_block.len {
                if !self
                    .posting_reader
                    .decode_one_position_block(0, position_list_block)?
                {
                    return Ok(END_POSITION);
                }
            }

            self.current_position += position_list_block.positions[self.position_block_cursor];
            self.position_block_cursor += 1;
        }

        return Ok(self.current_position);
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::{
        postings::{BuildingPostingWriter, PostingFormat, PostingIterator},
        DocId, DOC_LIST_BLOCK_LEN, END_DOCID, END_POSITION,
    };

    #[test]
    fn test_seek_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list().clone();

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
            .map(|(i, _)| (i % 3 + 1) as u32)
            .collect();
        let termfreqs = &termfreqs[..];

        for i in 0..termfreqs[0] {
            posting_writer.add_pos(0, i)?;
        }
        posting_writer.end_doc(docids[0])?;

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.seek(1)?, END_DOCID);

        for i in 0..termfreqs[1] {
            posting_writer.add_pos(0, i)?;
        }
        posting_writer.end_doc(docids[1])?;

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.seek(1)?, docids[1]);
        assert_eq!(posting_iterator.seek(docids[1] + 1)?, END_DOCID);
        assert_eq!(posting_iterator.seek(1)?, END_DOCID);

        for i in 2..BLOCK_LEN {
            for t in 0..termfreqs[i] {
                posting_writer.add_pos(0, t)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        // seek one by one

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        for &docid in &docids[..BLOCK_LEN] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        assert_eq!(posting_iterator.seek(docids[BLOCK_LEN - 1] + 1)?, END_DOCID);

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        // skip some items
        //
        for (i, &docid) in docids[..BLOCK_LEN].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
            }
        }

        for i in 0..BLOCK_LEN + 3 {
            for t in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(0, t)?;
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN])?;
        }

        // seek one by one

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        for &docid in &docids[..BLOCK_LEN * 2 + 3] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        assert_eq!(
            posting_iterator.seek(docids[BLOCK_LEN * 2 + 3 - 1] + 1)?,
            END_DOCID
        );

        // skip some items

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
            }
        }

        // skip some blocks

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        let docid = docids[BLOCK_LEN + 3];
        assert_eq!(posting_iterator.seek(docid)?, docid);

        Ok(())
    }

    #[test]
    fn test_seek_pos() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder()
            .with_tflist()
            .with_position_list()
            .build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list().clone();

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
            .map(|(i, _)| (3 + i % 3) as u32)
            .collect();
        let termfreqs = &termfreqs[..];

        for i in 0..termfreqs[0] {
            posting_writer.add_pos(0, i * 2)?;
        }
        posting_writer.end_doc(docids[0])?;

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        assert_eq!(posting_iterator.seek(0)?, 0);

        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, END_POSITION);
        // }
        assert_eq!(posting_iterator.seek(1)?, END_DOCID);

        for i in 0..termfreqs[1] {
            posting_writer.add_pos(0, i * 2)?;
        }
        posting_writer.end_doc(docids[1])?;

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.seek(1)?, docids[1]);
        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, 6);
        assert_eq!(posting_iterator.seek_pos(7)?, END_POSITION);

        assert_eq!(posting_iterator.seek(docids[1] + 1)?, END_DOCID);
        assert_eq!(posting_iterator.seek(1)?, END_DOCID);

        for i in 2..BLOCK_LEN {
            for t in 0..termfreqs[i] {
                posting_writer.add_pos(0, t * 2)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        // seek one by one

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        for &docid in &docids[..BLOCK_LEN] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        assert_eq!(posting_iterator.seek(docids[BLOCK_LEN - 1] + 1)?, END_DOCID);

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        // skip some items
        //
        for (i, &docid) in docids[..BLOCK_LEN].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
            }
        }

        for i in 0..BLOCK_LEN + 3 {
            for t in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(0, t * 2)?;
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN])?;
        }

        // seek one by one

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        for &docid in &docids[..BLOCK_LEN * 2 + 3] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        assert_eq!(
            posting_iterator.seek(docids[BLOCK_LEN * 2 + 3 - 1] + 1)?,
            END_DOCID
        );

        // skip some items

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
            }
        }

        // skip some blocks

        let mut posting_iterator = PostingIterator::open_building_posting_list(&posting_list);

        let docid = docids[BLOCK_LEN + 3];
        assert_eq!(posting_iterator.seek(docid)?, docid);
        let mut pos: u32 = 0;
        for t in 0..termfreqs[BLOCK_LEN + 3] {
            assert_eq!(posting_iterator.seek_pos(pos)?, t * 2);
            pos = t * 2 + 1;
        }
        assert_eq!(posting_iterator.seek_pos(pos)?, END_POSITION);

        Ok(())
    }
}
