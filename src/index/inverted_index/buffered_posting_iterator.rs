use std::io;

use crate::{
    index::PostingIterator,
    postings::{positions::PositionListBlock, DocListBlock, PostingFormat},
    DocId, END_DOCID, END_POSITION, INVALID_DOCID, INVALID_POSITION,
};

use super::{inverted_index_posting_reader::InvertedIndexPostingReader, SegmentPosting};

pub struct BufferedPostingIterator<'a> {
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
    posting_reader: InvertedIndexPostingReader<'a>,
    posting_format: PostingFormat,
}

impl<'a> BufferedPostingIterator<'a> {
    pub fn new(posting_format: PostingFormat, segment_postings: Vec<SegmentPosting<'a>>) -> Self {
        let doc_list_block = DocListBlock::new(posting_format.doc_list_format());
        let posting_reader = InvertedIndexPostingReader::new(segment_postings);

        Self {
            current_docid: u32::MAX,
            current_ttf: 0,
            current_tf: 0,
            need_decode_tf: false,
            need_decode_fieldmask: false,
            tf_buffer_cursor: 0,
            doc_buffer_cursor: 0,
            doc_list_block,
            position_docid: INVALID_DOCID,
            current_position: INVALID_POSITION,
            current_position_index: 0,
            position_block_cursor: 0,
            position_list_block: None,
            posting_reader,
            posting_format,
        }
    }

    fn decode_doc_buffer(&mut self, docid: DocId) -> io::Result<bool> {
        if !self
            .posting_reader
            .decode_doc_buffer(docid, &mut self.doc_list_block)?
        {
            return Ok(false);
        }
        self.current_docid = self.doc_list_block.base_docid + self.doc_list_block.docids[0];
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

    fn decode_doc_position_buffer(&mut self) -> io::Result<bool> {
        if self.position_list_block.is_none() {
            self.position_list_block = Some(Box::new(PositionListBlock::new()));
        }
        let position_list_block = self.position_list_block.as_mut().unwrap();

        if self.position_block_cursor == position_list_block.len
            || self.current_ttf >= position_list_block.start_ttf + (position_list_block.len as u64)
        {
            if !self
                .posting_reader
                .decode_position_buffer(self.current_ttf, position_list_block)?
            {
                return Ok(false);
            }
        }
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

    fn move_to_current_doc(&mut self) -> io::Result<bool> {
        if self.position_docid != self.current_docid {
            self.position_docid = self.current_docid;
            if !self.decode_tf_buffer()? {
                return Ok(false);
            }
            self.get_current_tf()?;
            self.get_current_ttf()?;
            self.decode_doc_position_buffer()
        } else {
            Ok(true)
        }
    }

    pub fn get_current_tf(&mut self) -> io::Result<u32> {
        self.decode_tf_buffer()?;
        self.current_tf =
            self.doc_list_block.termfreqs.as_deref().unwrap()[self.doc_buffer_cursor - 1];
        Ok(self.current_tf)
    }

    pub fn get_current_ttf(&mut self) -> io::Result<u64> {
        self.decode_tf_buffer()?;
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
}

impl<'a> PostingIterator for BufferedPostingIterator<'a> {
    /// SAFETY: param docid < END_DOCID && current_docid < END_DOCID
    fn seek(&mut self, docid: crate::DocId) -> io::Result<crate::DocId> {
        if self.current_docid != INVALID_DOCID && docid <= self.current_docid {
            return Ok(self.current_docid);
        }

        if self.doc_buffer_cursor == self.doc_list_block.len
            || self.doc_list_block.last_docid < docid
        {
            if !self.decode_doc_buffer(docid)? {
                self.current_docid = END_DOCID;
                return Ok(END_DOCID);
            }
        }

        while self.current_docid < docid {
            self.current_docid += self.doc_list_block.docids[self.doc_buffer_cursor];
            self.doc_buffer_cursor += 1;
        }

        Ok(self.current_docid)
    }

    fn seek_pos(&mut self, pos: u32) -> io::Result<u32> {
        if !self.posting_format.has_tflist() || !self.posting_format.has_position_list() {
            return Ok(END_POSITION);
        }

        if self.current_docid >= END_DOCID {
            return Ok(END_POSITION);
        }

        if self.position_docid != self.current_docid {
            if !self.move_to_current_doc()? {
                self.current_position = END_POSITION;
                return Ok(END_POSITION);
            }
        }

        while self.current_position < pos {
            let position_list_block = self.position_list_block.as_mut().unwrap();

            if self.position_block_cursor == position_list_block.len {
                if !self.decode_next_position_record()? {
                    self.current_position = END_POSITION;
                    return Ok(END_POSITION);
                }
                continue;
            }

            self.current_position_index += 1;
            if self.current_position_index == self.current_tf {
                self.current_position = END_POSITION;
                return Ok(END_POSITION);
            }

            self.current_position += position_list_block.positions[self.position_block_cursor];
            self.position_block_cursor += 1;
        }

        Ok(self.current_position)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::io;

    use crate::{
        index::{
            inverted_index::{BufferedPostingIterator, SegmentPosting},
            PostingIterator,
        },
        postings::{BuildingPostingWriter, PostingFormat},
        DocId, DOC_LIST_BLOCK_LEN, END_DOCID, END_POSITION, INVALID_DOCID,
    };

    #[test]
    fn test_single_segment() -> io::Result<()> {
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

        let building_segment = SegmentPosting::new_building_segment(0, &posting_list);
        let segment_postings = vec![building_segment];
        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        assert_eq!(posting_iterator.seek(0)?, 0);

        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, END_POSITION);

        assert_eq!(posting_iterator.seek(1)?, END_DOCID);

        for i in 0..termfreqs[1] {
            posting_writer.add_pos(0, i * 2)?;
        }
        posting_writer.end_doc(docids[1])?;

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.seek(1)?, docids[1]);
        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, 6);
        assert_eq!(posting_iterator.seek_pos(7)?, END_POSITION);

        assert_eq!(posting_iterator.seek(docids[1] + 1)?, END_DOCID);

        assert_eq!(posting_iterator.seek(1)?, END_DOCID);
        assert_eq!(posting_iterator.seek(INVALID_DOCID)?, END_DOCID);

        for i in 2..BLOCK_LEN {
            for t in 0..termfreqs[i] {
                posting_writer.add_pos(0, t * 2)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        // seek one by one

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for &docid in &docids[..BLOCK_LEN] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        assert_eq!(posting_iterator.seek(docids[BLOCK_LEN - 1] + 1)?, END_DOCID);

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

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

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for &docid in &docids[..BLOCK_LEN * 2 + 3] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        assert_eq!(
            posting_iterator.seek(docids[BLOCK_LEN * 2 + 3 - 1] + 1)?,
            END_DOCID
        );

        // skip some items

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
            }
        }

        // skip some blocks

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        let docid = docids[BLOCK_LEN + 3];
        assert_eq!(posting_iterator.seek(docid)?, docid);
        let mut pos: u32 = 0;
        for t in 0..termfreqs[BLOCK_LEN + 3] {
            assert_eq!(posting_iterator.seek_pos(pos)?, t * 2);
            pos = t * 2 + 1;
        }
        assert_eq!(posting_iterator.seek_pos(pos)?, END_POSITION);

        // seek INVALID_DOCID

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());
        assert_eq!(posting_iterator.seek(INVALID_DOCID)?, END_DOCID);
        assert_eq!(posting_iterator.seek(INVALID_DOCID)?, END_DOCID);
        assert_eq!(posting_iterator.seek(END_DOCID)?, END_DOCID);

        // seek END_DOCID

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());
        assert_eq!(posting_iterator.seek(END_DOCID)?, END_DOCID);
        assert_eq!(posting_iterator.seek(END_DOCID)?, END_DOCID);
        assert_eq!(posting_iterator.seek(INVALID_DOCID)?, END_DOCID);

        Ok(())
    }

    #[test]
    fn test_two_segments() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder()
            .with_tflist()
            .with_position_list()
            .build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list().clone();

        let mut posting_writer2: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list2 = posting_writer2.building_posting_list().clone();

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
        let positions: Vec<Vec<_>> = termfreqs
            .iter()
            .map(|&tf| (0..tf).map(|i| (i * 2) as u32).collect())
            .collect();

        for i in 0..termfreqs[0] {
            posting_writer.add_pos(0, i * 2)?;
        }
        posting_writer.end_doc(docids[0])?;

        for i in 0..termfreqs[0] {
            posting_writer2.add_pos(0, i * 2)?;
        }
        posting_writer2.end_doc(docids[0])?;

        let second_segment_basedocid = docids.last().unwrap() + 1;
        let building_segment = SegmentPosting::new_building_segment(0, &posting_list);
        let building_segment2 =
            SegmentPosting::new_building_segment(second_segment_basedocid, &posting_list2);
        let segment_postings = vec![building_segment, building_segment2];
        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        assert_eq!(posting_iterator.seek(0)?, 0);

        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, END_POSITION);

        assert_eq!(posting_iterator.seek(1)?, second_segment_basedocid);
        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, END_POSITION);

        for i in 0..termfreqs[1] {
            posting_writer.add_pos(0, i * 2)?;
        }
        posting_writer.end_doc(docids[1])?;
        for i in 0..termfreqs[1] {
            posting_writer2.add_pos(0, i * 2)?;
        }
        posting_writer2.end_doc(docids[1])?;

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.seek(1)?, docids[1]);
        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, 6);
        assert_eq!(posting_iterator.seek_pos(7)?, END_POSITION);

        let docid = posting_iterator.seek(docids[1] + 1)?;
        assert_eq!(docid, second_segment_basedocid + docids[0]);
        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, END_POSITION);
        let docid = posting_iterator.seek(docid + 1)?;
        assert_eq!(docid, second_segment_basedocid + docids[1]);

        for i in 2..BLOCK_LEN {
            for t in 0..termfreqs[i] {
                posting_writer.add_pos(0, t * 2)?;
            }
            posting_writer.end_doc(docids[i])?;
        }
        for i in 2..BLOCK_LEN {
            for t in 0..termfreqs[i] {
                posting_writer2.add_pos(0, t * 2)?;
            }
            posting_writer2.end_doc(docids[i])?;
        }

        // seek one by one

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for &docid in &docids[..BLOCK_LEN] {
            assert_eq!(posting_iterator.seek(docid)?, docid);
        }
        let docid = posting_iterator.seek(docids[BLOCK_LEN - 1] + 1)?;
        assert_eq!(docid, second_segment_basedocid);
        assert_eq!(posting_iterator.seek_pos(0)?, 0);
        assert_eq!(posting_iterator.seek_pos(1)?, 2);
        assert_eq!(posting_iterator.seek_pos(3)?, 4);
        assert_eq!(posting_iterator.seek_pos(5)?, END_POSITION);

        // skip some items

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for (i, &docid) in docids[..BLOCK_LEN].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
                if i % 4 == 0 {
                    assert_eq!(posting_iterator.seek_pos(0)?, 0);
                    assert_eq!(posting_iterator.seek_pos(1)?, 2);
                    assert_eq!(posting_iterator.seek_pos(3)?, 4);
                    let mut pos = posting_iterator.seek_pos(5)?;
                    if i % 3 > 0 {
                        assert_eq!(pos, 6);
                        pos = posting_iterator.seek_pos(pos + 1)?;
                        if i % 3 > 1 {
                            assert_eq!(pos, 8);
                            pos = posting_iterator.seek_pos(pos + 1)?;
                        }
                    }
                    assert_eq!(pos, END_POSITION);
                }
            }
        }

        for i in 0..BLOCK_LEN + 3 {
            for t in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(0, t * 2)?;
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN])?;
        }
        for i in 0..BLOCK_LEN + 3 {
            for t in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer2.add_pos(0, t * 2)?;
            }
            posting_writer2.end_doc(docids[i + BLOCK_LEN])?;
        }

        // seek one by one

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            assert_eq!(posting_iterator.seek(docid)?, docid);
            let mut doc_positions = vec![];
            let mut pos = 0;
            loop {
                pos = posting_iterator.seek_pos(pos)?;
                if pos == END_POSITION {
                    break;
                }
                doc_positions.push(pos);
                pos += 1;
            }
            assert_eq!(doc_positions, positions[i]);
        }
        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            assert_eq!(
                posting_iterator.seek(docid + second_segment_basedocid)?,
                docid + second_segment_basedocid
            );
            let mut doc_positions = vec![];
            let mut pos = 0;
            loop {
                pos = posting_iterator.seek_pos(pos)?;
                if pos == END_POSITION {
                    break;
                }
                doc_positions.push(pos);
                pos += 1;
            }
            assert_eq!(doc_positions, positions[i]);
        }
        assert_eq!(
            posting_iterator.seek(docids.last().unwrap().clone() + second_segment_basedocid + 1)?,
            END_DOCID
        );

        // skip some items

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
                let mut doc_positions = vec![];
                let mut pos = 0;
                loop {
                    pos = posting_iterator.seek_pos(pos)?;
                    if pos == END_POSITION {
                        break;
                    }
                    doc_positions.push(pos);
                    pos += 1;
                }
                assert_eq!(doc_positions, positions[i]);
            }
        }

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            if i % 3 == 0 {
                assert_eq!(
                    posting_iterator.seek(docid + second_segment_basedocid)?,
                    docid + second_segment_basedocid
                );
                let mut doc_positions = vec![];
                let mut pos = 0;
                loop {
                    pos = posting_iterator.seek_pos(pos)?;
                    if pos == END_POSITION {
                        break;
                    }
                    doc_positions.push(pos);
                    pos += 1;
                }
                assert_eq!(doc_positions, positions[i]);
            }
        }

        // skip some blocks

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        let docid = docids[BLOCK_LEN + 3];
        assert_eq!(posting_iterator.seek(docid)?, docid);
        let mut doc_positions = vec![];
        let mut pos = 0;
        loop {
            pos = posting_iterator.seek_pos(pos)?;
            if pos == END_POSITION {
                break;
            }
            doc_positions.push(pos);
            pos += 1;
        }
        assert_eq!(doc_positions, positions[BLOCK_LEN + 3]);

        let docid = docids[BLOCK_LEN + 3];
        assert_eq!(
            posting_iterator.seek(docid + second_segment_basedocid)?,
            docid + second_segment_basedocid
        );
        let mut doc_positions = vec![];
        let mut pos = 0;
        loop {
            pos = posting_iterator.seek_pos(pos)?;
            if pos == END_POSITION {
                break;
            }
            doc_positions.push(pos);
            pos += 1;
        }
        assert_eq!(doc_positions, positions[BLOCK_LEN + 3]);

        // skip one segment

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        let docid = docids[BLOCK_LEN + 3];
        assert_eq!(
            posting_iterator.seek(docid + second_segment_basedocid)?,
            docid + second_segment_basedocid
        );
        let mut doc_positions = vec![];
        let mut pos = 0;
        loop {
            pos = posting_iterator.seek_pos(pos)?;
            if pos == END_POSITION {
                break;
            }
            doc_positions.push(pos);
            pos += 1;
        }
        assert_eq!(doc_positions, positions[BLOCK_LEN + 3]);

        Ok(())
    }

    #[test]
    fn test_get_tf_and_fieldmask() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder()
            .with_tflist()
            .with_fieldmask()
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

        let mut rng = rand::thread_rng();

        let termfreqs: Vec<u32> = (0..BLOCK_LEN * 2 + 3)
            .map(|_| rng.gen_range(1..=64))
            .collect();
        let termfreqs = &termfreqs[..];

        let mut positions: Vec<Vec<u32>> = vec![];
        let mut field_indexes: Vec<Vec<usize>> = vec![];
        let mut fieldmasks: Vec<u8> = vec![];

        for &tf in termfreqs {
            let mut mask = 0;
            let mut field = rng.gen_range(0..8) as usize;
            mask |= 1 << field;
            let mut indexes = vec![field];
            let mut pos = 0;
            let mut one_positions: Vec<u32> = vec![pos];
            for _ in 1..tf {
                if field < 7 {
                    field += 1;
                    mask |= 1 << field;
                }
                indexes.push(field);
                pos += 1;
                one_positions.push(pos);
            }
            positions.push(one_positions);
            field_indexes.push(indexes);
            fieldmasks.push(mask);
        }

        for t in 0..termfreqs[0] {
            posting_writer.add_pos(field_indexes[0][t as usize], positions[0][t as usize])?;
        }
        posting_writer.end_doc(docids[0])?;

        let building_segment = SegmentPosting::new_building_segment(0, &posting_list);
        let segment_postings = vec![building_segment];

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        assert_eq!(posting_iterator.seek(0)?, 0);

        assert_eq!(posting_iterator.get_current_tf()?, termfreqs[0]);
        assert_eq!(posting_iterator.get_current_fieldmask()?, fieldmasks[0]);

        assert_eq!(posting_iterator.seek(1)?, END_DOCID);

        // Skip tf to get fm
        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.get_current_fieldmask()?, fieldmasks[0]);
        assert_eq!(posting_iterator.seek(1)?, END_DOCID);

        for t in 0..termfreqs[1] {
            posting_writer.add_pos(field_indexes[1][t as usize], positions[1][t as usize])?;
        }
        posting_writer.end_doc(docids[1])?;

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        // Skip one doc to get tf
        assert_eq!(posting_iterator.seek(0)?, 0);
        assert_eq!(posting_iterator.seek(1)?, docids[1]);

        assert_eq!(posting_iterator.seek(docids[1] + 1)?, END_DOCID);
        assert_eq!(posting_iterator.get_current_tf()?, termfreqs[1]);
        assert_eq!(posting_iterator.get_current_fieldmask()?, fieldmasks[1]);

        for i in 2..BLOCK_LEN {
            for t in 0..termfreqs[i] {
                posting_writer.add_pos(field_indexes[i][t as usize], positions[i][t as usize])?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        // seek one by one

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for (i, &docid) in docids[..BLOCK_LEN].iter().enumerate() {
            assert_eq!(posting_iterator.seek(docid)?, docid);
            assert_eq!(posting_iterator.get_current_tf()?, termfreqs[i]);
            assert_eq!(posting_iterator.get_current_fieldmask()?, fieldmasks[i]);
        }
        assert_eq!(posting_iterator.seek(docids[BLOCK_LEN - 1] + 1)?, END_DOCID);

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        // skip some items

        for (i, &docid) in docids[..BLOCK_LEN].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
                assert_eq!(posting_iterator.get_current_fieldmask()?, fieldmasks[i]);
            }
        }

        for i in 0..BLOCK_LEN + 3 {
            for t in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(
                    field_indexes[i + BLOCK_LEN][t as usize],
                    positions[i + BLOCK_LEN][t as usize],
                )?;
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN])?;
        }

        // seek one by one

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            assert_eq!(posting_iterator.seek(docid)?, docid);
            // Just fieldmask
            assert_eq!(posting_iterator.get_current_fieldmask()?, fieldmasks[i]);
        }
        assert_eq!(
            posting_iterator.seek(docids[BLOCK_LEN * 2 + 3 - 1] + 1)?,
            END_DOCID
        );

        // skip some items

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        for (i, &docid) in docids[..BLOCK_LEN * 2 + 3].iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(posting_iterator.seek(docid)?, docid);
                // Just tf
                assert_eq!(posting_iterator.get_current_tf()?, termfreqs[i]);
            }
        }

        // skip some blocks

        let mut posting_iterator =
            BufferedPostingIterator::new(posting_format.clone(), segment_postings.clone());

        let docid = docids[BLOCK_LEN + 3];
        assert_eq!(posting_iterator.seek(docid)?, docid);
        assert_eq!(posting_iterator.get_current_tf()?, termfreqs[BLOCK_LEN + 3]);
        assert_eq!(
            posting_iterator.get_current_fieldmask()?,
            fieldmasks[BLOCK_LEN + 3]
        );

        Ok(())
    }
}
