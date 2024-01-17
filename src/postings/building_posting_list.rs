use std::{io, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use crate::DocId;

use super::{
    posting_writer::{BuildingPostingBlock, FlushInfo, PostingWriter},
    skip_list::{BuildingSkipList, BuildingSkipListReader, BuildingSkipListWriter},
    ByteSliceList, ByteSliceReader, ByteSliceWriter, PostingBlock, PostingBlockSnapshot,
    PostingFormat, PostingRead, PostingReader,
};

pub struct BuildingPostingList<A: Allocator = Global> {
    building_block: Arc<BuildingPostingBlock>,
    flush_info: Arc<FlushInfo>,
    byte_slice_list: Arc<ByteSliceList<A>>,
    posting_format: PostingFormat,
    building_skip_list: BuildingSkipList<A>,
}

pub struct BuildingPostingWriter<A: Allocator = Global> {
    posting_writer: PostingWriter<ByteSliceWriter<A>, BuildingSkipListWriter<A>>,
    byte_slice_list: Arc<ByteSliceList<A>>,
    building_skip_list: BuildingSkipList<A>,
}

pub struct BuildingPostingReader<'a> {
    last_docid: DocId,
    read_count: usize,
    doc_count: usize,
    building_block_snapshot: PostingBlockSnapshot,
    posting_reader: PostingReader<ByteSliceReader<'a>, BuildingSkipListReader<'a>>,
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
        let skip_list_writer =
            BuildingSkipListWriter::new_in(skip_list_format, initial_slice_capacity, allocator);
        let building_skip_list = skip_list_writer.building_skip_list();
        let posting_writer =
            PostingWriter::new_with_skip_list(posting_format, byte_slice_writer, skip_list_writer);

        Self {
            posting_writer,
            byte_slice_list,
            building_skip_list,
        }
    }

    pub fn building_posting_list(&self) -> BuildingPostingList<A> {
        BuildingPostingList {
            building_block: self.posting_writer.building_block().clone(),
            flush_info: self.posting_writer.flush_info().clone(),
            byte_slice_list: self.byte_slice_list.clone(),
            posting_format: self.posting_writer.posting_format().clone(),
            building_skip_list: self.building_skip_list.clone(),
        }
    }

    pub fn add_pos(&mut self, field: usize) {
        self.posting_writer.add_pos(field);
    }

    pub fn end_doc(&mut self, docid: DocId) {
        self.posting_writer.end_doc(docid);
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
        let mut doc_count = flush_info.flushed_count();
        let mut byte_slice_reader = if doc_count == 0 {
            ByteSliceReader::empty()
        } else {
            ByteSliceReader::open(byte_slice_list)
        };
        let mut building_block_snapshot = building_block.snapshot(flush_info.buffer_len());
        let doc_count_updated = building_posting_list.flush_info.flushed_count();
        if doc_count < doc_count_updated {
            building_block_snapshot.clear();
            doc_count = doc_count_updated;
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        }

        let skip_list_reader =
            BuildingSkipListReader::open(&building_posting_list.building_skip_list);
        let posting_reader = PostingReader::open_with_skip_list(
            posting_format,
            doc_count,
            byte_slice_reader,
            skip_list_reader,
        );

        doc_count += building_block_snapshot.len();

        Self {
            last_docid: 0,
            read_count: 0,
            doc_count,
            building_block_snapshot,
            posting_reader,
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

    // pub fn seek(&mut self, docid: DocId, posting_block: &mut PostingBlock) -> io::Result<bool> {
    //     if self.eof() {
    //         return Ok(false);
    //     }

    //     if !self.posting_reader.eof() {
    //         let ok = self.posting_reader.seek(docid, posting_block)?;
    //         self.last_docid = self.posting_reader.last_docid();
    //         self.read_count = self.posting_reader.read_count();
    //         if ok {
    //             return Ok(true);
    //         }
    //         debug_assert!(self.posting_reader.eof());
    //     }

    //     self.decode_one_block(posting_block)?;

    //     Ok(posting_block.len > 0 && posting_block.last_docid() >= docid)
    // }
}

impl<'a> PostingRead for BuildingPostingReader<'a> {
    fn posting_format(&self) -> &PostingFormat {
        self.posting_reader.posting_format()
    }

    fn decode_one_block(&mut self, posting_block: &mut PostingBlock) -> io::Result<()> {
        posting_block.len = 0;

        if self.eof() {
            return Ok(());
        }

        if !self.posting_reader.eof() {
            self.posting_reader.decode_one_block(posting_block)?;
            self.last_docid = posting_block.last_docid();
            self.read_count += posting_block.len;
            return Ok(());
        }

        self.building_block_snapshot.copy_to(posting_block);
        posting_block.decode_docids(self.last_docid);
        self.last_docid = posting_block.last_docid();
        self.read_count += posting_block.len;

        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use std::{io, thread};

    use crate::{
        postings::{
            BuildingPostingReader, BuildingPostingWriter, PostingBlock, PostingFormat, PostingRead,
        },
        DocId, TermFreq, POSTING_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list();
        let mut posting_block = PostingBlock::new(&posting_format);
        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.doc_count, 0);
        assert_eq!(posting_reader.read_count, 0);
        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, 0);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.doc_count, 0);
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
            .map(|(i, _)| (i % 3 + 1) as TermFreq)
            .collect();
        let termfreqs = &termfreqs[..];

        for _ in 0..termfreqs[0] {
            posting_writer.add_pos(1);
        }
        posting_writer.end_doc(docids[0]);

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.doc_count, 1);
        assert_eq!(posting_reader.read_count, 0);
        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, 1);
        assert_eq!(posting_block.docids[0], docids[0]);
        assert_eq!(posting_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);

        assert!(posting_reader.eof());
        assert_eq!(posting_reader.doc_count, 1);
        assert_eq!(posting_reader.read_count, 1);

        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, 0);

        for _ in 0..termfreqs[1] {
            posting_writer.add_pos(1);
        }
        posting_writer.end_doc(docids[1]);

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, 2);
        assert_eq!(posting_block.docids[0], docids[0]);
        assert_eq!(posting_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);
        assert_eq!(posting_block.docids[1], docids[1]);
        assert_eq!(posting_block.termfreqs.as_ref().unwrap()[1], termfreqs[1]);

        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, 0);

        for i in 2..BLOCK_LEN {
            for _ in 0..termfreqs[i] {
                posting_writer.add_pos(1);
            }
            posting_writer.end_doc(docids[i]);
        }

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, BLOCK_LEN);
        assert_eq!(posting_block.docids, &docids[0..BLOCK_LEN]);
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, 0);

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(1);
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN]);
        }

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, BLOCK_LEN);
        assert_eq!(posting_block.docids, &docids[0..BLOCK_LEN]);
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, BLOCK_LEN);
        assert_eq!(posting_block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, 3);
        assert_eq!(
            &posting_block.docids[0..3],
            &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &posting_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        posting_reader.decode_one_block(&mut posting_block)?;
        assert_eq!(posting_block.len, 0);

        Ok(())
    }

    #[test]
    fn test_multithread() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list();

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

        thread::scope(|scope| {
            let w = scope.spawn(move || {
                for i in 0..BLOCK_LEN * 2 + 3 {
                    for _ in 0..termfreqs[i] {
                        posting_writer.add_pos(1);
                    }
                    posting_writer.end_doc(docids[i]);
                    thread::yield_now();
                }
            });

            let r = scope.spawn(move || loop {
                let mut posting_block = PostingBlock::new(&posting_format);
                let mut posting_reader = BuildingPostingReader::open(&posting_list);
                let mut offset = 0;
                loop {
                    posting_reader.decode_one_block(&mut posting_block).unwrap();
                    if posting_block.len == 0 {
                        break;
                    }
                    let block_len = posting_block.len;
                    assert_eq!(
                        &posting_block.docids[0..block_len],
                        &docids[offset..offset + block_len]
                    );

                    assert_eq!(
                        &posting_block.termfreqs.as_ref().unwrap()[0..block_len],
                        &termfreqs[offset..offset + block_len]
                    );
                    offset += block_len;
                }
                if posting_reader.doc_count == BLOCK_LEN * 2 + 3 {
                    break;
                }
                thread::yield_now();
            });

            w.join().unwrap();
            r.join().unwrap();
        });

        Ok(())
    }

    // #[test]
    // fn test_seek_basic() -> io::Result<()> {
    //     const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
    //     let posting_format = PostingFormat::builder().with_tflist().build();
    //     let mut posting_writer: BuildingPostingWriter =
    //         BuildingPostingWriter::new(posting_format.clone(), 1024);
    //     let posting_list = posting_writer.building_posting_list();
    //     let mut posting_block = PostingBlock::new(&posting_format);

    //     let docids: Vec<_> = (0..BLOCK_LEN * 2 + 3)
    //         .enumerate()
    //         .map(|(i, _)| (i * 5 + i % 3) as DocId)
    //         .collect();
    //     let docids = &docids[..];
    //     let termfreqs: Vec<_> = (0..BLOCK_LEN * 2 + 3)
    //         .enumerate()
    //         .map(|(i, _)| (i % 3 + 1) as TermFreq)
    //         .collect();
    //     let termfreqs = &termfreqs[..];

    //     for _ in 0..termfreqs[0] {
    //         posting_writer.add_pos(1);
    //     }
    //     posting_writer.end_doc(docids[0]);

    //     let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //     assert!(posting_reader.seek(0, &mut posting_block)?);
    //     assert_eq!(posting_block.len, 1);
    //     assert_eq!(posting_block.docids[0], docids[0]);
    //     assert_eq!(posting_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);

    //     for _ in 0..termfreqs[1] {
    //         posting_writer.add_pos(1);
    //     }
    //     posting_writer.end_doc(docids[1]);

    //     let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //     assert!(posting_reader.seek(0, &mut posting_block)?);
    //     assert_eq!(posting_block.len, 2);
    //     assert_eq!(posting_block.docids[0], docids[0]);
    //     assert_eq!(posting_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);
    //     assert_eq!(posting_block.docids[1], docids[1]);
    //     assert_eq!(posting_block.termfreqs.as_ref().unwrap()[1], termfreqs[1]);

    //     assert!(posting_reader.eof());
    //     assert!(!posting_reader.seek(0, &mut posting_block)?);

    //     for i in 2..BLOCK_LEN {
    //         for _ in 0..termfreqs[i] {
    //             posting_writer.add_pos(1);
    //         }
    //         posting_writer.end_doc(docids[i]);
    //     }

    //     let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //     assert!(posting_reader.seek(docids[BLOCK_LEN - 1], &mut posting_block)?);
    //     assert_eq!(posting_block.len, BLOCK_LEN);
    //     assert_eq!(posting_block.docids, &docids[0..BLOCK_LEN]);
    //     assert_eq!(
    //         &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
    //         &termfreqs[0..BLOCK_LEN]
    //     );

    //     assert!(posting_reader.eof());
    //     assert!(!posting_reader.seek(0, &mut posting_block)?);

    //     for i in 0..BLOCK_LEN + 3 {
    //         for _ in 0..termfreqs[i + BLOCK_LEN] {
    //             posting_writer.add_pos(1);
    //         }
    //         posting_writer.end_doc(docids[i + BLOCK_LEN]);
    //     }

    //     // block one by one
    //     let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //     assert!(posting_reader.seek(docids[BLOCK_LEN - 1], &mut posting_block)?);
    //     assert_eq!(posting_block.len, BLOCK_LEN);
    //     assert_eq!(posting_block.docids, &docids[0..BLOCK_LEN]);
    //     assert_eq!(
    //         &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
    //         &termfreqs[0..BLOCK_LEN]
    //     );

    //     assert!(posting_reader.seek(docids[BLOCK_LEN * 2 - 1], &mut posting_block)?);
    //     assert_eq!(posting_block.len, BLOCK_LEN);
    //     assert_eq!(posting_block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
    //     assert_eq!(
    //         &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
    //         &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
    //     );

    //     assert!(posting_reader.seek(docids.last().cloned().unwrap(), &mut posting_block)?);
    //     assert_eq!(posting_block.len, 3);
    //     assert_eq!(
    //         &posting_block.docids[0..3],
    //         &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //     );
    //     assert_eq!(
    //         &posting_block.termfreqs.as_ref().unwrap()[0..3],
    //         &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //     );

    //     assert!(posting_reader.eof());
    //     assert!(!posting_reader.seek(0, &mut posting_block)?);

    //     // skip some block
    //     let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //     assert!(posting_reader.seek(docids[BLOCK_LEN * 2 - 1], &mut posting_block)?);
    //     assert_eq!(posting_block.len, BLOCK_LEN);
    //     assert_eq!(posting_block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
    //     assert_eq!(
    //         &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
    //         &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
    //     );

    //     assert!(posting_reader.seek(docids.last().cloned().unwrap(), &mut posting_block)?);
    //     assert_eq!(posting_block.len, 3);
    //     assert_eq!(
    //         &posting_block.docids[0..3],
    //         &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //     );
    //     assert_eq!(
    //         &posting_block.termfreqs.as_ref().unwrap()[0..3],
    //         &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //     );

    //     assert!(posting_reader.eof());
    //     assert!(!posting_reader.seek(0, &mut posting_block)?);

    //     // seek the last block
    //     let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //     assert!(posting_reader.seek(docids.last().cloned().unwrap(), &mut posting_block)?);
    //     assert_eq!(posting_block.len, 3);
    //     assert_eq!(
    //         &posting_block.docids[0..3],
    //         &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //     );
    //     assert_eq!(
    //         &posting_block.termfreqs.as_ref().unwrap()[0..3],
    //         &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //     );

    //     assert!(posting_reader.eof());
    //     assert!(!posting_reader.seek(0, &mut posting_block)?);

    //     // seek eof
    //     let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //     assert!(!posting_reader.eof());
    //     assert!(!posting_reader.seek(DocId::MAX, &mut posting_block)?);
    //     assert!(posting_reader.eof());

    //     Ok(())
    // }

    // #[test]
    // fn test_seek_multi_thread() -> io::Result<()> {
    //     const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
    //     let posting_format = PostingFormat::builder().with_tflist().build();
    //     let mut posting_writer: BuildingPostingWriter =
    //         BuildingPostingWriter::new(posting_format.clone(), 1024);
    //     let posting_list = posting_writer.building_posting_list();

    //     let docids: Vec<_> = (0..BLOCK_LEN * 2 + 3)
    //         .enumerate()
    //         .map(|(i, _)| (i * 5 + i % 3) as DocId)
    //         .collect();
    //     let docids = &docids[..];
    //     let termfreqs: Vec<_> = (0..BLOCK_LEN * 2 + 3)
    //         .enumerate()
    //         .map(|(i, _)| (i % 3 + 1) as TermFreq)
    //         .collect();
    //     let termfreqs = &termfreqs[..];

    //     thread::scope(|scope| {
    //         let w = scope.spawn(move || {
    //             for i in 0..BLOCK_LEN * 2 + 3 {
    //                 for _ in 0..termfreqs[i] {
    //                     posting_writer.add_pos(1);
    //                 }
    //                 posting_writer.end_doc(docids[i]);
    //                 thread::yield_now();
    //             }
    //         });

    //         let r = scope.spawn(move || loop {
    //             let mut posting_block = PostingBlock::new(&posting_format);
    //             let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //             if posting_reader.doc_count() == BLOCK_LEN * 2 + 3 {
    //                 // block one by one
    //                 let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //                 assert!(posting_reader
    //                     .seek(docids[BLOCK_LEN - 1], &mut posting_block)
    //                     .unwrap());
    //                 assert_eq!(posting_block.len, BLOCK_LEN);
    //                 assert_eq!(posting_block.docids, &docids[0..BLOCK_LEN]);
    //                 assert_eq!(
    //                     &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
    //                     &termfreqs[0..BLOCK_LEN]
    //                 );

    //                 assert!(posting_reader
    //                     .seek(docids[BLOCK_LEN * 2 - 1], &mut posting_block)
    //                     .unwrap());
    //                 assert_eq!(posting_block.len, BLOCK_LEN);
    //                 assert_eq!(posting_block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
    //                 assert_eq!(
    //                     &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
    //                     &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
    //                 );

    //                 assert!(posting_reader
    //                     .seek(docids.last().cloned().unwrap(), &mut posting_block)
    //                     .unwrap());
    //                 assert_eq!(posting_block.len, 3);
    //                 assert_eq!(
    //                     &posting_block.docids[0..3],
    //                     &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //                 );
    //                 assert_eq!(
    //                     &posting_block.termfreqs.as_ref().unwrap()[0..3],
    //                     &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //                 );

    //                 assert!(posting_reader.eof());
    //                 assert!(!posting_reader.seek(0, &mut posting_block).unwrap());

    //                 // skip some block
    //                 let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //                 assert!(posting_reader
    //                     .seek(docids[BLOCK_LEN * 2 - 1], &mut posting_block)
    //                     .unwrap());
    //                 assert_eq!(posting_block.len, BLOCK_LEN);
    //                 assert_eq!(posting_block.docids, &docids[BLOCK_LEN..BLOCK_LEN * 2]);
    //                 assert_eq!(
    //                     &posting_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
    //                     &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
    //                 );

    //                 assert!(posting_reader
    //                     .seek(docids.last().cloned().unwrap(), &mut posting_block)
    //                     .unwrap());
    //                 assert_eq!(posting_block.len, 3);
    //                 assert_eq!(
    //                     &posting_block.docids[0..3],
    //                     &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //                 );
    //                 assert_eq!(
    //                     &posting_block.termfreqs.as_ref().unwrap()[0..3],
    //                     &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //                 );

    //                 assert!(posting_reader.eof());
    //                 assert!(!posting_reader.seek(0, &mut posting_block).unwrap());

    //                 // seek the last block
    //                 let mut posting_reader = BuildingPostingReader::open(&posting_list);

    //                 assert!(posting_reader
    //                     .seek(docids.last().cloned().unwrap(), &mut posting_block)
    //                     .unwrap());
    //                 assert_eq!(posting_block.len, 3);
    //                 assert_eq!(
    //                     &posting_block.docids[0..3],
    //                     &docids[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //                 );
    //                 assert_eq!(
    //                     &posting_block.termfreqs.as_ref().unwrap()[0..3],
    //                     &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
    //                 );

    //                 assert!(posting_reader.eof());
    //                 assert!(!posting_reader.seek(0, &mut posting_block).unwrap());

    //                 // seek eof
    //                 let mut posting_reader = BuildingPostingReader::open(&posting_list);
    //                 assert!(!posting_reader.eof());
    //                 assert!(!posting_reader.seek(DocId::MAX, &mut posting_block).unwrap());
    //                 assert!(posting_reader.eof());

    //                 break;
    //             }

    //             thread::yield_now();
    //         });

    //         w.join().unwrap();
    //         r.join().unwrap();
    //     });

    //     Ok(())
    // }
}
