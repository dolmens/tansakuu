use std::{io, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use crate::DocId;

use super::{
    posting_writer::{BuildingPostingBlock, FlushInfo, PostingWriter},
    ByteSliceList, ByteSliceReader, ByteSliceWriter, PostingBlock, PostingBlockSnapshot,
    PostingFormat, PostingReader,
};

pub struct BuildingPostingList<A: Allocator = Global> {
    building_block: Arc<BuildingPostingBlock>,
    flush_info: Arc<FlushInfo>,
    byte_slice_list: Arc<ByteSliceList<A>>,
    posting_format: PostingFormat,
}

pub struct BuildingPostingWriter<A: Allocator = Global> {
    posting_writer: PostingWriter<ByteSliceWriter<A>>,
    byte_slice_list: Arc<ByteSliceList<A>>,
}

pub struct BuildingPostingReader<'a> {
    last_docid: DocId,
    read_count: usize,
    doc_count: usize,
    block: PostingBlockSnapshot,
    posting_reader: PostingReader<ByteSliceReader<'a>>,
}

impl<A: Allocator + Default> BuildingPostingWriter<A> {
    pub fn new(posting_format: PostingFormat, initial_slice_capacity: usize) -> Self {
        Self::new_in(posting_format, initial_slice_capacity, A::default())
    }
}

impl<A: Allocator> BuildingPostingWriter<A> {
    pub fn new_in(
        posting_format: PostingFormat,
        initial_slice_capacity: usize,
        allocator: A,
    ) -> Self {
        let byte_slice_writer =
            ByteSliceWriter::with_initial_capacity_in(initial_slice_capacity, allocator);
        let byte_slice_list = byte_slice_writer.byte_slice_list();
        let posting_writer = PostingWriter::new(posting_format, byte_slice_writer);

        Self {
            posting_writer,
            byte_slice_list,
        }
    }

    pub fn building_posting_list(&self) -> BuildingPostingList<A> {
        BuildingPostingList {
            building_block: self.posting_writer.building_block().clone(),
            flush_info: self.posting_writer.flush_info().clone(),
            byte_slice_list: self.byte_slice_list.clone(),
            posting_format: self.posting_writer.posting_format().clone(),
        }
    }

    pub fn add_pos(&mut self, field: usize) {
        self.posting_writer.add_pos(field);
    }

    pub fn end_doc(&mut self, docid: DocId) {
        self.posting_writer.end_doc(docid);
    }

    pub fn flush(&mut self) -> io::Result<usize> {
        self.posting_writer.flush()
    }
}

impl<'a> BuildingPostingReader<'a> {
    pub fn open<A: Allocator>(building_posting_data: &'a BuildingPostingList<A>) -> Self {
        let flush_info = building_posting_data.flush_info.as_ref();
        let byte_slice_list = building_posting_data.byte_slice_list.as_ref();
        let building_block = building_posting_data.building_block.as_ref();
        let posting_format = building_posting_data.posting_format.clone();
        let mut doc_count = flush_info.doc_count();
        let mut byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        let mut block = building_block.snapshot();
        let doc_count_updated = flush_info.doc_count();
        if doc_count < doc_count_updated {
            block.clear();
            doc_count = doc_count_updated;
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        }
        let posting_reader = PostingReader::open(doc_count, posting_format, byte_slice_reader);

        doc_count += block.len();

        Self {
            last_docid: 0,
            read_count: 0,
            doc_count,
            block,
            posting_reader,
        }
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.doc_count
    }

    pub fn decode_one_block(&mut self, posting_block: &mut PostingBlock) -> io::Result<()> {
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

        self.block.copy_to(posting_block);
        posting_block.decode(self.last_docid);
        self.last_docid = posting_block.last_docid();
        self.read_count += posting_block.len;

        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use std::{io, thread};

    use crate::{
        postings::{BuildingPostingReader, BuildingPostingWriter, PostingBlock, PostingFormat},
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

        let docids: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| (i * 5 + i % 3) as DocId)
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

        let docids: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| (i * 5 + i % 3) as DocId)
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
}
