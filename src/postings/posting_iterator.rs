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
    need_decode_tf: bool,
    need_decode_fieldmask: bool,
    tf_buffer_cursor: usize,
    doc_buffer_cursor: usize,
    doc_list_block: DocListBlock,
    position_docid: DocId,
    current_position: u32,
    current_position_index: u32,
    position_block_cursor: usize,
    position_list_block: Option<Box<PositionListBlock>>,
    posting_reader: R,
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

        Self {
            current_docid: INVALID_DOCID,
            current_ttf: 0,
            current_tf: 0,
            need_decode_tf: false,
            need_decode_fieldmask: false,
            tf_buffer_cursor: 0,
            doc_buffer_cursor: 0,
            doc_list_block,
            position_docid: INVALID_DOCID,
            current_position: 0,
            current_position_index: 0,
            position_block_cursor: 0,
            position_list_block: None,
            posting_reader,
            posting_format,
        }
    }

    pub fn seek(&mut self, docid: DocId) -> io::Result<DocId> {
        if self.doc_buffer_cursor == self.doc_list_block.len
            || docid > self.doc_list_block.last_docid
        {
            if !self.decode_doc_buffer(docid)? {
                return Ok(END_DOCID);
            }
        }

        while self.current_docid < docid {
            self.current_docid += self.doc_list_block.docids[self.doc_buffer_cursor] as DocId;
            self.doc_buffer_cursor += 1;
        }

        Ok(self.current_docid)
    }

    pub fn seek_pos(&mut self, pos: u32) -> io::Result<u32> {
        if !self.posting_format.has_tflist() || !self.posting_format.has_position_list() {
            return Ok(END_POSITION);
        }

        if self.current_docid >= END_DOCID {
            return Ok(END_POSITION);
        }

        if self.position_docid != self.current_docid {
            if !self.move_to_current_doc()? {
                return Ok(END_POSITION);
            }
        }

        while self.current_position < pos {
            let position_list_block = self.position_list_block.as_deref_mut().unwrap();

            if self.position_block_cursor == position_list_block.len {
                if !self.decode_next_position_record()? {
                    return Ok(END_POSITION);
                }
                continue;
            }

            self.current_position_index += 1;
            if self.current_position_index == self.current_tf {
                return Ok(END_POSITION);
            }

            self.current_position += position_list_block.positions[self.position_block_cursor];
            self.position_block_cursor += 1;
        }

        return Ok(self.current_position);
    }

    fn decode_doc_buffer(&mut self, docid: DocId) -> io::Result<bool> {
        if !self
            .posting_reader
            .decode_doc_buffer(docid, &mut self.doc_list_block)?
        {
            self.current_docid = END_DOCID;
            return Ok(false);
        }
        self.current_docid =
            self.doc_list_block.base_docid + self.doc_list_block.docids[0] as DocId;
        if self.posting_format.has_tflist() {
            self.current_ttf = self.doc_list_block.base_ttf;
        }
        self.doc_buffer_cursor = 1;
        self.need_decode_tf = true;
        self.need_decode_fieldmask = true;

        Ok(true)
    }

    fn decode_tf_buffer(&mut self) -> io::Result<bool> {
        if self.need_decode_tf {
            self.need_decode_tf = false;
            if !self
                .posting_reader
                .decode_tf_buffer(&mut self.doc_list_block)?
            {
                return Ok(false);
            }
            self.tf_buffer_cursor = 0;
        }

        Ok(true)
    }

    fn decode_fieldmask_buffer(&mut self) -> io::Result<bool> {
        if self.need_decode_fieldmask {
            self.need_decode_fieldmask = false;
            self.posting_reader
                .decode_fieldmask_buffer(&mut self.doc_list_block)
        } else {
            Ok(false)
        }
    }

    pub fn get_current_tf(&mut self) -> io::Result<u32> {
        self.decode_tf_buffer()?;
        debug_assert!(self.doc_buffer_cursor > 0);
        self.current_tf =
            self.doc_list_block.termfreqs.as_deref().unwrap()[self.doc_buffer_cursor - 1];
        Ok(self.current_tf)
    }

    pub fn get_current_ttf(&mut self) -> io::Result<u64> {
        self.decode_tf_buffer()?;
        debug_assert!(self.doc_buffer_cursor > 0);
        while self.tf_buffer_cursor < self.doc_buffer_cursor - 1 {
            self.current_ttf +=
                self.doc_list_block.termfreqs.as_deref().unwrap()[self.tf_buffer_cursor] as u64;
            self.tf_buffer_cursor += 1;
        }
        Ok(self.current_ttf)
    }

    pub fn get_current_fieldmask(&mut self) -> io::Result<u8> {
        if self.posting_format.has_fieldmask() {
            self.decode_tf_buffer()?;
            self.decode_fieldmask_buffer()?;
            let fieldmask =
                self.doc_list_block.fieldmasks.as_deref().unwrap()[self.doc_buffer_cursor - 1];
            Ok(fieldmask)
        } else {
            Ok(0)
        }
    }

    fn move_to_current_doc(&mut self) -> io::Result<bool> {
        if !self.decode_tf_buffer()? {
            return Ok(false);
        }
        self.get_current_tf()?;
        self.get_current_ttf()?;
        self.decode_position_buffer()
    }

    fn decode_position_buffer(&mut self) -> io::Result<bool> {
        if self.position_list_block.is_none() {
            self.position_list_block = Some(Box::new(PositionListBlock::new()));
        }
        let position_list_block = self.position_list_block.as_deref_mut().unwrap();

        if self.position_block_cursor == position_list_block.len
            || self.current_ttf > position_list_block.start_ttf + (position_list_block.len as u64)
        {
            if !self
                .posting_reader
                .decode_position_buffer(self.current_ttf, position_list_block)?
            {
                return Ok(false);
            }
        }
        self.position_docid = self.current_docid;
        self.position_block_cursor = (self.current_ttf - position_list_block.start_ttf) as usize;
        self.current_position = position_list_block.positions[self.position_block_cursor];
        self.current_position_index = 0;
        self.position_block_cursor += 1;

        Ok(true)
    }

    fn decode_next_position_record(&mut self) -> io::Result<bool> {
        let position_list_block = self.position_list_block.as_mut().unwrap();

        if !self
            .posting_reader
            .decode_next_position_record(position_list_block)?
        {
            return Ok(false);
        }
        self.current_position += position_list_block.positions[0];
        self.position_block_cursor = 1;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::{
        postings::{BuildingPostingWriter, PostingFormat, PostingIterator},
        DocId, DocId32, DOC_LIST_BLOCK_LEN, END_DOCID, END_POSITION,
    };

    #[test]
    fn test_seek_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone());
        let posting_list = posting_writer.building_posting_list().clone();

        let docids_deltas: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as DocId32).collect();
        let docids_deltas = &docids_deltas[..];
        let docids: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .map(|docid| docid as DocId)
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
            BuildingPostingWriter::new(posting_format.clone());
        let posting_list = posting_writer.building_posting_list().clone();

        let docids_deltas: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as DocId32).collect();
        let docids_deltas = &docids_deltas[..];
        let docids: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .map(|docid| docid as DocId)
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
