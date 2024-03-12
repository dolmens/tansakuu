use std::{collections::BinaryHeap, io};

use crate::{
    index::inverted_index::SegmentMultiPostingData,
    postings::{BuildingPostingReader, DocListBlock, DocListFormat, PostingRead},
    DocId, END_DOCID,
};

use super::{
    persistent_segment_posting_reader::PersistentSegmentPostingReader, BuildingSegmentPosting,
    PersistentSegmentPosting, SegmentMultiPosting,
};

pub struct PostingSegmentMultiReader<'a> {
    initialized: bool,
    base_docid: DocId,
    pick_heap: BinaryHeap<PostingPick>,
    posting_count: usize,
    doc_list_blocks: Vec<DocListBlock>,
    inner_reader: SegmentMultiReaderInner<'a>,
}

#[derive(Debug, PartialEq, Eq)]
struct PostingPick {
    current_docid: DocId,
    posting_index: usize,
    doc_buffer_cursor: usize,
}

impl PartialOrd for PostingPick {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.current_docid.partial_cmp(&self.current_docid)
    }
}

impl Ord for PostingPick {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.current_docid.cmp(&self.current_docid)
    }
}

impl PostingPick {
    fn new(current_docid: DocId, posting_index: usize) -> Self {
        Self {
            current_docid,
            posting_index,
            doc_buffer_cursor: 1,
        }
    }
}

enum SegmentMultiReaderInner<'a> {
    Persistent(PersistentSegmentMultiReader<'a>),
    Building(BuildingSegmentMultiReader<'a>),
}

struct PersistentSegmentMultiReader<'a> {
    posting_readers: Vec<PersistentSegmentPostingReader<'a>>,
}

struct BuildingSegmentMultiReader<'a> {
    posting_readers: Vec<BuildingPostingReader<'a>>,
}

impl<'a> PostingSegmentMultiReader<'a> {
    pub fn open(
        doc_list_format: DocListFormat,
        segment_multi_posting: &'static SegmentMultiPosting<'a>,
    ) -> Self {
        let base_docid = segment_multi_posting.base_docid();
        let posting_count = segment_multi_posting.posting_count();
        let doc_list_blocks = (0..posting_count)
            .map(|_| DocListBlock::new(&doc_list_format))
            .collect();
        let inner_reader = SegmentMultiReaderInner::open(segment_multi_posting);
        let heap = BinaryHeap::with_capacity(posting_count);

        Self {
            initialized: false,
            base_docid,
            pick_heap: heap,
            posting_count,
            doc_list_blocks,
            inner_reader,
        }
    }

    // Every time this seek will consume one docid
    pub fn seek(&mut self, docid: DocId) -> io::Result<DocId> {
        if !self.initialized {
            self.initialized = true;
            self.init_read(docid)?;
            if self.pick_heap.is_empty() {
                return Ok(END_DOCID);
            }
        }

        loop {
            if self.pick_heap.is_empty() {
                return Ok(END_DOCID);
            }
            let mut posting_pick = self.pick_heap.pop().unwrap();
            let current_docid = posting_pick.current_docid;
            let posting_index = posting_pick.posting_index;
            let doc_list_block = &mut self.doc_list_blocks[posting_index];
            let doc_buffer_cursor = posting_pick.doc_buffer_cursor;
            if doc_buffer_cursor < doc_list_block.len {
                posting_pick.current_docid =
                    current_docid + doc_list_block.docids[doc_buffer_cursor];
                posting_pick.doc_buffer_cursor += 1;
                self.pick_heap.push(posting_pick);
            } else {
                let start_docid = if docid > self.base_docid {
                    docid - self.base_docid
                } else {
                    0
                };
                if self.inner_reader.decode_doc_buffer(
                    posting_index,
                    start_docid,
                    doc_list_block,
                )? {
                    doc_list_block.base_docid += self.base_docid;
                    doc_list_block.last_docid += self.base_docid;
                    posting_pick.current_docid =
                        doc_list_block.base_docid + doc_list_block.docids[0];
                    posting_pick.doc_buffer_cursor = 1;
                    self.pick_heap.push(posting_pick);
                }
            }
            if docid <= current_docid {
                return Ok(current_docid);
            }
        }
    }

    fn init_read(&mut self, docid: DocId) -> io::Result<()> {
        let docid = if docid > self.base_docid {
            docid - self.base_docid
        } else {
            0
        };

        for posting_index in 0..self.posting_count {
            let doc_list_block = &mut self.doc_list_blocks[posting_index];
            if self
                .inner_reader
                .decode_doc_buffer(posting_index, docid, doc_list_block)?
            {
                doc_list_block.base_docid += self.base_docid;
                doc_list_block.last_docid += self.base_docid;
                let current_docid = doc_list_block.base_docid + doc_list_block.docids[0];
                self.pick_heap
                    .push(PostingPick::new(current_docid, posting_index));
            }
        }

        Ok(())
    }
}

impl<'a> SegmentMultiReaderInner<'a> {
    pub fn open(segment_multi_posting: &'static SegmentMultiPosting<'a>) -> Self {
        match segment_multi_posting.posting_data() {
            SegmentMultiPostingData::Persistent(segment_multi_posting) => {
                Self::Persistent(PersistentSegmentMultiReader::open(segment_multi_posting))
            }
            SegmentMultiPostingData::Building(segment_multi_posting) => {
                Self::Building(BuildingSegmentMultiReader::open(segment_multi_posting))
            }
        }
    }

    pub fn decode_doc_buffer(
        &mut self,
        posting_index: usize,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        match self {
            Self::Persistent(reader) => {
                reader.decode_doc_buffer(posting_index, docid, doc_list_block)
            }
            Self::Building(reader) => {
                reader.decode_doc_buffer(posting_index, docid, doc_list_block)
            }
        }
    }
}

impl<'a> PersistentSegmentMultiReader<'a> {
    pub fn open(segment_multi_posting: &'static Vec<PersistentSegmentPosting<'a>>) -> Self {
        let posting_readers = segment_multi_posting
            .iter()
            .map(|posting| {
                PersistentSegmentPostingReader::open(posting.term_info.clone(), posting.index_data)
            })
            .collect();
        Self { posting_readers }
    }

    pub fn decode_doc_buffer(
        &mut self,
        posting_index: usize,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.posting_readers[posting_index].decode_doc_buffer(docid, doc_list_block)
    }
}

impl<'a> BuildingSegmentMultiReader<'a> {
    pub fn open(segment_multi_posting: &'static Vec<BuildingSegmentPosting<'a>>) -> Self {
        let posting_readers = segment_multi_posting
            .iter()
            .map(|posting| BuildingPostingReader::open(posting.building_posting_list))
            .collect();

        Self { posting_readers }
    }

    pub fn decode_doc_buffer(
        &mut self,
        posting_index: usize,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.posting_readers[posting_index].decode_doc_buffer(docid, doc_list_block)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BinaryHeap, io, mem::MaybeUninit};

    use crate::{
        index::inverted_index::{
            segment_posting::BuildingSegmentPosting, SegmentMultiPosting, SegmentMultiPostingData,
        },
        postings::{BuildingPostingWriter, DocListBlock, DocListFormat, PostingFormat},
        DocId, DOC_LIST_BLOCK_LEN, END_DOCID,
    };

    use super::{PostingPick, PostingSegmentMultiReader};

    #[test]
    fn test_posting_pick_ord() {
        let mut heap = BinaryHeap::new();
        heap.push(PostingPick::new(1, 0));
        heap.push(PostingPick::new(3, 1));
        heap.push(PostingPick::new(2, 2));
        let expect = vec![1, 2, 3];
        let mut got = vec![];
        while !heap.is_empty() {
            let pick = heap.pop().unwrap();
            got.push(pick.current_docid);
        }
        assert_eq!(got, expect);
    }

    #[test]
    fn test_traverse_buffer() -> io::Result<()> {
        let doc_list_format = DocListFormat::builder().build();
        let mut pick_heap = BinaryHeap::new();
        let mut doc_list_blocks: Vec<_> = (0..3)
            .map(|_| DocListBlock::new(&doc_list_format))
            .collect();

        doc_list_blocks[0].base_docid = 10;
        doc_list_blocks[0].docids[0] = 1;
        doc_list_blocks[0].docids[1] = 10;
        doc_list_blocks[0].len = 2;
        doc_list_blocks[0].last_docid = 21;
        pick_heap.push(PostingPick::new(11, 0));

        doc_list_blocks[1].base_docid = 10;
        doc_list_blocks[1].docids[0] = 3;
        doc_list_blocks[1].docids[1] = 2;
        doc_list_blocks[1].docids[2] = 2;
        doc_list_blocks[1].docids[3] = 10;
        doc_list_blocks[1].len = 4;
        doc_list_blocks[1].last_docid = 27;
        pick_heap.push(PostingPick::new(13, 1));

        doc_list_blocks[2].base_docid = 10;
        doc_list_blocks[2].docids[0] = 2;
        doc_list_blocks[2].docids[1] = 4;
        doc_list_blocks[2].docids[2] = 10;
        doc_list_blocks[2].len = 3;
        doc_list_blocks[2].last_docid = 26;
        pick_heap.push(PostingPick::new(12, 2));

        let mut uninit_reader = MaybeUninit::<PostingSegmentMultiReader>::uninit();
        let reader = unsafe { &mut (*uninit_reader.as_mut_ptr()) };
        unsafe {
            std::ptr::write(&mut reader.initialized, true);
            std::ptr::write(&mut reader.base_docid, 10);
            std::ptr::write(&mut reader.pick_heap, pick_heap);
            std::ptr::write(&mut reader.posting_count, 3);
            std::ptr::write(&mut reader.doc_list_blocks, doc_list_blocks);
        }

        let expect = vec![11, 12, 13, 15, 16, 17];
        let mut docids = vec![];
        let mut docid = 0;
        for _ in 0..6 {
            docid = reader.seek(docid)?;
            docids.push(docid);
        }
        assert_eq!(docids, expect);

        unsafe {
            let _ = std::ptr::read(&mut reader.pick_heap);
            let _ = std::ptr::read(&mut reader.doc_list_blocks);
        }

        Ok(())
    }

    #[test]
    fn test_complete_process() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::default();
        let doc_list_format = posting_format.doc_list_format().clone();
        let mut posting_writer1 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer1.add_pos(0, 0)?;
            posting_writer1.end_doc((i * 3) as DocId)?;
        }
        let mut posting_writer2 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer2.add_pos(0, 0)?;
            posting_writer2.end_doc((i * 3 + 2) as DocId)?;
        }
        let mut posting_writer3 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer3.add_pos(0, 0)?;
            posting_writer3.end_doc((i * 3 + 1) as DocId)?;
        }
        let segment_posting1 = BuildingSegmentPosting {
            building_posting_list: posting_writer1.building_posting_list(),
        };
        let segment_posting2 = BuildingSegmentPosting {
            building_posting_list: posting_writer2.building_posting_list(),
        };
        let segment_posting3 = BuildingSegmentPosting {
            building_posting_list: posting_writer3.building_posting_list(),
        };
        let base_docid = 10;
        let multi_posting_data = SegmentMultiPostingData::Building(vec![
            segment_posting1,
            segment_posting2,
            segment_posting3,
        ]);

        let segment_multi_posting = SegmentMultiPosting::new(base_docid, multi_posting_data);

        let mut reader = PostingSegmentMultiReader::open(doc_list_format, unsafe {
            std::mem::transmute(&segment_multi_posting)
        });

        let mut docids = vec![];
        let mut docid = 0;
        loop {
            docid = reader.seek(docid)?;
            if docid == END_DOCID {
                break;
            }
            docids.push(docid);
        }
        let expect: Vec<_> = (0..((BLOCK_LEN + 3) * 3) as DocId)
            .map(|docid| docid + base_docid)
            .collect();
        assert_eq!(docids, expect);

        Ok(())
    }
}