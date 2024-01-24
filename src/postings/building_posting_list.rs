use std::{io, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use crate::DocId;

use super::{
    positions::{BuildingPositionList, BuildingPositionListReader, BuildingPositionListWriter},
    posting_writer::{BuildingPostingBlock, FlushInfo, PostingWriter},
    skip_list::{BuildingSkipList, BuildingSkipListReader, BuildingSkipListWriter},
    ByteSliceList, ByteSliceReader, ByteSliceWriter, PostingBlock, PostingBlockSnapshot,
    PostingFormat, PostingRead, PostingReader,
};

#[derive(Clone)]
pub struct BuildingPostingList<A: Allocator = Global> {
    flush_info: Arc<FlushInfo>,
    building_block: Arc<BuildingPostingBlock>,
    byte_slice_list: Arc<ByteSliceList<A>>,
    building_position_list: Option<BuildingPositionList<A>>,
    building_skip_list: BuildingSkipList<A>,
    posting_format: PostingFormat,
}

pub struct BuildingPostingWriter<A: Allocator = Global> {
    posting_writer:
        PostingWriter<ByteSliceWriter<A>, BuildingSkipListWriter<A>, BuildingPositionListWriter<A>>,
    building_posting_list: BuildingPostingList<A>,
}

pub struct BuildingPostingReader<'a> {
    read_count: usize,
    flushed_count: usize,
    building_block_snapshot: PostingBlockSnapshot,
    posting_reader: PostingReader<
        ByteSliceReader<'a>,
        BuildingSkipListReader<'a>,
        BuildingPositionListReader<'a>,
    >,
}

impl<A: Allocator + Clone + Default> BuildingPostingWriter<A> {
    pub fn new(posting_format: PostingFormat, initial_slice_capacity: usize) -> Self {
        Self::new_in(posting_format, initial_slice_capacity, A::default())
    }
}

impl<A: Allocator + Clone> BuildingPostingWriter<A> {
    pub fn new_in(
        posting_format: PostingFormat,
        initial_slice_capacity: usize,
        allocator: A,
    ) -> Self {
        let byte_slice_writer =
            ByteSliceWriter::with_initial_capacity_in(initial_slice_capacity, allocator.clone());
        let byte_slice_list = byte_slice_writer.byte_slice_list();
        let skip_list_format = posting_format.skip_list_format().clone();
        let skip_list_writer = BuildingSkipListWriter::new_in(
            skip_list_format,
            initial_slice_capacity,
            allocator.clone(),
        );
        let (position_list_writer, building_position_list) = if posting_format.has_position_list() {
            let position_list_writer = BuildingPositionListWriter::new_in(allocator.clone());
            let building_position_list = position_list_writer.building_position_list().clone();
            (Some(position_list_writer), Some(building_position_list))
        } else {
            (None, None)
        };
        let building_skip_list = skip_list_writer.building_skip_list().clone();
        let posting_writer = PostingWriter::new(
            posting_format.clone(),
            byte_slice_writer,
            skip_list_writer,
            position_list_writer,
        );
        let flush_info = posting_writer.flush_info().clone();
        let building_block = posting_writer.building_block().clone();

        let building_posting_list = BuildingPostingList {
            flush_info,
            building_block,
            byte_slice_list,
            building_position_list,
            building_skip_list,
            posting_format,
        };

        Self {
            posting_writer,
            building_posting_list,
        }
    }

    pub fn building_posting_list(&self) -> &BuildingPostingList<A> {
        &self.building_posting_list
    }

    pub fn add_pos(&mut self, field: usize, pos: u32) -> io::Result<()> {
        self.posting_writer.add_pos(field, pos)
    }

    pub fn end_doc(&mut self, docid: DocId) -> io::Result<()> {
        self.posting_writer.end_doc(docid)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.posting_writer.flush()
    }
}

impl<'a> BuildingPostingReader<'a> {
    pub fn open<A: Allocator>(building_posting_list: &'a BuildingPostingList<A>) -> Self {
        let flush_info = building_posting_list.flush_info.load();
        let byte_slice_list = building_posting_list.byte_slice_list.as_ref();
        let building_block = building_posting_list.building_block.as_ref();
        let posting_format = building_posting_list.posting_format.clone();
        let mut flushed_count = flush_info.flushed_count();
        let mut byte_slice_reader = if flushed_count == 0 {
            ByteSliceReader::empty()
        } else {
            ByteSliceReader::open(byte_slice_list)
        };
        let mut building_block_snapshot = building_block.snapshot(flush_info.buffer_len());
        let flushed_count_updated = building_posting_list.flush_info.load().flushed_count();
        if flushed_count < flushed_count_updated {
            building_block_snapshot.clear();
            flushed_count = flushed_count_updated;
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        }

        let skip_list_reader =
            BuildingSkipListReader::open(&building_posting_list.building_skip_list);

        let position_list_reader = building_posting_list
            .building_position_list
            .as_ref()
            .map(|building_position_list| BuildingPositionListReader::open(building_position_list));

        let posting_reader = PostingReader::open(
            posting_format,
            flushed_count,
            byte_slice_reader,
            skip_list_reader,
            position_list_reader,
        );

        Self {
            read_count: 0,
            flushed_count,
            building_block_snapshot,
            posting_reader,
        }
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.flushed_count + self.building_block_snapshot.len()
    }

    pub fn doc_count(&self) -> usize {
        self.flushed_count + self.building_block_snapshot.len()
    }
}

impl<'a> PostingRead for BuildingPostingReader<'a> {
    fn decode_one_block(
        &mut self,
        docid: DocId,
        posting_block: &mut PostingBlock,
    ) -> io::Result<bool> {
        if self.eof() {
            return Ok(false);
        }

        if self.read_count < self.flushed_count {
            if self.posting_reader.decode_one_block(docid, posting_block)? {
                self.read_count += posting_block.len;
                return Ok(true);
            }
        }

        self.read_count = self.flushed_count;

        if self.building_block_snapshot.len() == 0 {
            return Ok(false);
        }

        self.read_count += self.building_block_snapshot.len();

        let mut last_docid = self.posting_reader.last_docid();
        let base_docid = last_docid;
        self.building_block_snapshot.copy_to(posting_block);
        for i in 0..posting_block.len {
            last_docid += posting_block.docids[i];
        }
        if last_docid < docid {
            return Ok(false);
        }

        posting_block.base_docid = base_docid;
        posting_block.last_docid = last_docid;
        posting_block.base_ttf = self.posting_reader.last_ttf();

        Ok(true)
    }

    fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut super::positions::PositionListBlock,
    ) -> io::Result<bool> {
        self.posting_reader
            .decode_one_position_block(from_ttf, position_list_block)
    }
}

#[cfg(test)]
mod tests {
    use std::{io, thread};

    use crate::{
        postings::{
            positions::PositionListBlock, BuildingPostingReader, BuildingPostingWriter,
            PostingBlock, PostingFormat, PostingRead,
        },
        DocId, POSITION_BLOCK_LEN, POSTING_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list().clone();
        let mut posting_block = PostingBlock::new(&posting_format);
        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), 0);
        assert_eq!(posting_reader.read_count, 0);
        assert!(!posting_reader.decode_one_block(0, &mut posting_block)?);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), 0);
        assert_eq!(posting_reader.read_count, 0);

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

        for _ in 0..termfreqs[0] {
            posting_writer.add_pos(0, 1)?;
        }
        posting_writer.end_doc(docids[0])?;

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), 1);
        assert_eq!(posting_reader.read_count, 0);
        assert!(posting_reader.decode_one_block(0, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, 0);
        assert_eq!(posting_block.last_docid, docids[0]);
        assert_eq!(posting_block.len, 1);
        assert_eq!(posting_block.docids[0], docids[0]);
        assert_eq!(posting_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);

        assert!(posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), 1);
        assert_eq!(posting_reader.read_count, 1);

        assert!(!posting_reader.decode_one_block(docids[0], &mut posting_block)?);

        for _ in 0..termfreqs[1] {
            posting_writer.add_pos(0, 1)?;
        }
        posting_writer.end_doc(docids[1])?;

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.decode_one_block(0, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, 0);
        assert_eq!(posting_block.last_docid, docids[1]);
        assert_eq!(posting_block.len, 2);
        assert_eq!(posting_block.docids[0], docids_deltas[0]);
        assert_eq!(posting_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);
        assert_eq!(posting_block.docids[1], docids_deltas[1]);
        assert_eq!(posting_block.termfreqs.as_ref().unwrap()[1], termfreqs[1]);

        let block_last_docid = posting_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut posting_block)?);

        for i in 2..BLOCK_LEN {
            for _ in 0..termfreqs[i] {
                posting_writer.add_pos(0, 1)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.decode_one_block(0, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, 0);
        assert_eq!(posting_block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(posting_block.len, BLOCK_LEN);
        assert_eq!(posting_block.docids, &docids_deltas[0..BLOCK_LEN]);
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = posting_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut posting_block)?);

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(0, 1)?;
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN])?;
        }

        let mut posting_reader = BuildingPostingReader::open(&posting_list);

        assert!(posting_reader.decode_one_block(0, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, 0);
        assert_eq!(posting_block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(posting_block.len, BLOCK_LEN);
        assert_eq!(posting_block.docids, &docids_deltas[0..BLOCK_LEN]);
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = posting_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(posting_block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(posting_block.len, BLOCK_LEN);
        assert_eq!(
            posting_block.docids,
            &docids_deltas[BLOCK_LEN..BLOCK_LEN * 2]
        );
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = posting_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(posting_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(posting_block.len, 3);
        assert_eq!(
            &posting_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = posting_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut posting_block)?);

        // skip one block

        let mut posting_reader = BuildingPostingReader::open(&posting_list);

        assert!(posting_reader.decode_one_block(docids[BLOCK_LEN - 1] + 1, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(posting_block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(posting_block.len, BLOCK_LEN);
        assert_eq!(
            posting_block.docids,
            &docids_deltas[BLOCK_LEN..BLOCK_LEN * 2]
        );
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = posting_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(posting_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(posting_block.len, 3);
        assert_eq!(
            &posting_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = posting_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut posting_block)?);

        // skip two blocks

        let mut posting_reader = BuildingPostingReader::open(&posting_list);

        assert!(posting_reader.decode_one_block(docids[BLOCK_LEN * 2 - 1] + 1, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(posting_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(posting_block.len, 3);
        assert_eq!(
            &posting_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = posting_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut posting_block)?);

        // skip to end

        let mut posting_reader = BuildingPostingReader::open(&posting_list);

        assert!(!posting_reader
            .decode_one_block(docids.last().cloned().unwrap() + 1, &mut posting_block)?);

        Ok(())
    }

    #[test]
    fn test_multithread() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
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

        thread::scope(|scope| {
            let w = scope.spawn(move || {
                for i in 0..BLOCK_LEN * 2 + 3 {
                    for _ in 0..termfreqs[i] {
                        posting_writer.add_pos(0, 1).unwrap();
                    }
                    posting_writer.end_doc(docids[i]).unwrap();
                    thread::yield_now();
                }
            });

            let r = scope.spawn(move || loop {
                let mut posting_block = PostingBlock::new(&posting_format);
                let mut posting_reader = BuildingPostingReader::open(&posting_list);
                let mut query_docid = 0;
                let mut offset = 0;
                loop {
                    if posting_reader
                        .decode_one_block(query_docid, &mut posting_block)
                        .unwrap()
                    {
                        let block_len = posting_block.len;
                        let prev_docid = if offset > 0 { docids[offset - 1] } else { 0 };
                        assert_eq!(posting_block.base_docid, prev_docid);
                        assert_eq!(posting_block.last_docid, docids[offset + block_len - 1]);
                        assert_eq!(
                            &posting_block.docids[0..block_len],
                            &docids_deltas[offset..offset + block_len]
                        );

                        assert_eq!(
                            &posting_block.termfreqs.as_ref().unwrap()[0..block_len],
                            &termfreqs[offset..offset + block_len]
                        );
                        query_docid = posting_block.last_docid + 1;
                        offset += block_len;
                    } else {
                        break;
                    }
                }
                if posting_reader.doc_count() == BLOCK_LEN * 2 + 3 {
                    break;
                }
                thread::yield_now();
            });

            w.join().unwrap();
            r.join().unwrap();
        });

        Ok(())
    }

    #[test]
    fn test_with_position_list() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let posting_format = PostingFormat::builder()
            .with_tflist()
            .with_position_list()
            .build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list().clone();
        let mut posting_block = PostingBlock::new(&posting_format);
        let mut position_list_block = PositionListBlock::new();

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), 0);
        assert_eq!(posting_reader.read_count, 0);
        assert!(!posting_reader.decode_one_block(0, &mut posting_block)?);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), 0);
        assert_eq!(posting_reader.read_count, 0);

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

        let mut positions = vec![];
        let mut positions_deltas = vec![];
        for i in 0..BLOCK_LEN * 2 + 3 {
            let mut deltas = vec![];
            let mut ps = vec![];
            let mut p = 0;
            for j in 0..(i % 4) + 1 {
                let d = (i + j) as u32;
                deltas.push(d);
                p += d;
                ps.push(p);
            }
            positions_deltas.push(deltas);
            positions.push(ps);
        }

        let pos_delta_flatten: Vec<_> = positions_deltas.iter().flatten().cloned().collect();

        for &p in &positions[0] {
            posting_writer.add_pos(0, p)?;
        }
        posting_writer.end_doc(docids[0])?;

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), 1);
        assert_eq!(posting_reader.read_count, 0);
        assert!(posting_reader.decode_one_block(0, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, 0);
        assert_eq!(posting_block.last_docid, docids[0]);
        assert_eq!(posting_block.len, 1);
        assert_eq!(posting_block.docids[0], docids[0]);
        assert_eq!(
            posting_block.termfreqs.as_ref().unwrap()[0],
            positions[0].len() as u32
        );

        assert!(posting_reader.decode_one_position_block(0, &mut position_list_block)?);
        assert_eq!(
            &position_list_block.positions[0..position_list_block.len],
            &positions[0]
        );

        for &p in &positions[1] {
            posting_writer.add_pos(0, p)?;
        }
        posting_writer.end_doc(docids[1])?;

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), 2);
        assert_eq!(posting_reader.read_count, 0);
        assert!(posting_reader.decode_one_block(0, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, 0);
        assert_eq!(posting_block.last_docid, docids[1]);
        assert_eq!(posting_block.len, 2);
        assert_eq!(posting_block.docids[0], docids[0]);
        assert_eq!(
            posting_block.termfreqs.as_ref().unwrap()[0],
            positions[0].len() as u32
        );
        assert_eq!(posting_block.docids[1], docids[1]);
        assert_eq!(
            posting_block.termfreqs.as_ref().unwrap()[1],
            positions[1].len() as u32
        );

        assert!(posting_reader.decode_one_position_block(0, &mut position_list_block)?);
        let ttf = positions[0].len() + positions[1].len();
        assert_eq!(
            &position_list_block.positions[0..position_list_block.len],
            &pos_delta_flatten[0..ttf]
        );

        for i in 2..BLOCK_LEN {
            for &p in &positions[i] {
                posting_writer.add_pos(0, p)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.doc_count(), BLOCK_LEN);
        assert_eq!(posting_reader.read_count, 0);
        assert!(posting_reader.decode_one_block(0, &mut posting_block)?);
        assert_eq!(posting_block.base_docid, 0);
        assert_eq!(posting_block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(posting_block.len, BLOCK_LEN);

        let ttf: usize = positions[0..BLOCK_LEN].iter().map(|ps| ps.len()).sum();
        let mut current_ttf = 0;
        while current_ttf < ttf {
            let block_len = std::cmp::min(ttf - current_ttf, POSITION_BLOCK_LEN);
            assert!(posting_reader
                .decode_one_position_block(current_ttf as u64, &mut position_list_block)?);
            assert_eq!(position_list_block.len, block_len);
            assert_eq!(position_list_block.start_ttf, current_ttf as u64);
            assert_eq!(
                &position_list_block.positions[0..position_list_block.len],
                &pos_delta_flatten[current_ttf..current_ttf + block_len]
            );
            current_ttf += block_len;
        }

        Ok(())
    }
}
