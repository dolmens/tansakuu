use std::sync::Arc;

use allocator_api2::alloc::{Allocator, Global};

use crate::{
    postings::{compression, ByteSliceList, ByteSliceReader, ByteSliceWriter},
    util::{AcqRelUsize, RelaxedU32, RelaxedUsize},
    DocId, TermFreq, INVALID_DOCID, SKIPLIST_BLOCK_LEN,
};

use super::SkipListFormat;

pub struct BuildingSkipList<A: Allocator = Global> {
    building_block: BuildingSkipListBlock,
    flushed_size: AcqRelUsize,
    slice_list: Arc<ByteSliceList<A>>,
    skip_list_format: SkipListFormat,
}

pub struct BuildingSkipListBlock {
    len: AcqRelUsize,
    docids: [RelaxedU32; SKIPLIST_BLOCK_LEN],
    offsets: [RelaxedUsize; SKIPLIST_BLOCK_LEN],
    termfreqs: Option<Box<[RelaxedU32]>>,
}

pub struct SkipListBlockSnapshot {
    len: usize,
    docids: Box<[DocId]>,
    offsets: Box<[usize]>,
    termfreqs: Option<Box<[TermFreq]>>,
}

pub struct SkipListBlock {
    pub len: usize,
    pub docids: [DocId; SKIPLIST_BLOCK_LEN],
    pub offsets: [usize; SKIPLIST_BLOCK_LEN],
    pub termfreqs: Option<Box<[TermFreq]>>,
}

pub struct BuildingSkipListWriter<A: Allocator = Global> {
    last_docid: DocId,
    last_offset: usize,
    block_len: usize,
    flushed_size: usize,
    slice_writer: ByteSliceWriter<A>,
    building_skip_list: Arc<BuildingSkipList<A>>,
    skip_list_format: SkipListFormat,
}

pub struct BuildingSkipListReader<'a> {
    empty: bool,
    decoded: bool,
    current_docid: DocId,
    current_offset: usize,
    current_cursor: usize,
    skip_list_block: SkipListBlock,
    block_snapshot: SkipListBlockSnapshot,
    flushed_size: usize,
    slice_reader: ByteSliceReader<'a>,
    skip_list_format: SkipListFormat,
}

impl BuildingSkipListBlock {
    pub fn new(skip_list_format: &SkipListFormat) -> Self {
        let docids = std::iter::repeat_with(|| RelaxedU32::new(0))
            .take(SKIPLIST_BLOCK_LEN)
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap();
        let offsets = std::iter::repeat_with(|| RelaxedUsize::new(0))
            .take(SKIPLIST_BLOCK_LEN)
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap();
        let termfreqs = if skip_list_format.has_tflist() {
            Some(
                std::iter::repeat_with(|| RelaxedU32::new(0))
                    .take(SKIPLIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            len: AcqRelUsize::new(0),
            docids,
            offsets,
            termfreqs,
        }
    }

    pub fn len(&self) -> usize {
        self.len.load()
    }

    pub fn clear(&self) {
        self.len.store(0);
    }

    pub fn add_skip_item(
        &self,
        last_docid: DocId,
        offset: usize,
        index: usize,
        tf: Option<TermFreq>,
    ) {
        self.docids[index].store(last_docid);
        self.offsets[index].store(offset);
        if let Some(termfreqs) = self.termfreqs.as_deref() {
            termfreqs[index].store(tf.unwrap_or_default());
        }
    }
}

impl SkipListBlockSnapshot {
    fn with_capacity(capacity: usize, skip_list_format: &SkipListFormat) -> Self {
        let docids = vec![0; capacity].into_boxed_slice();
        let offsets = vec![0; capacity].into_boxed_slice();
        let termfreqs = if skip_list_format.has_tflist() {
            Some(vec![0; capacity].into_boxed_slice())
        } else {
            None
        };

        Self {
            len: 0,
            docids,
            offsets,
            termfreqs,
        }
    }

    fn snapshot(&mut self, building_block: &BuildingSkipListBlock, block_len: usize) {
        self.len = block_len;
        if block_len > 0 {
            self.docids[0..block_len]
                .iter_mut()
                .zip(building_block.docids[0..block_len].iter())
                .for_each(|(v, docid)| *v = docid.load());
            self.offsets[0..block_len]
                .iter_mut()
                .zip(building_block.offsets[0..block_len].iter())
                .for_each(|(v, offset)| *v = offset.load());
            self.termfreqs.as_deref_mut().map(|termfreqs| {
                termfreqs[0..block_len]
                    .iter_mut()
                    .zip(building_block.termfreqs.as_ref().unwrap()[0..block_len].iter())
                    .for_each(|(v, tf)| *v = tf.load())
            });
        }
    }

    fn copy_to(&self, skip_list_block: &mut SkipListBlock) {
        let len = self.len;
        skip_list_block.len = len;
        if len > 0 {
            skip_list_block.docids[0..len].copy_from_slice(&self.docids[0..len]);
            skip_list_block.offsets[0..len].copy_from_slice(&&self.offsets[0..len]);
            if let Some(termfreqs) = skip_list_block.termfreqs.as_deref_mut() {
                if let Some(mytermfreqs) = self.termfreqs.as_deref() {
                    termfreqs[0..len].copy_from_slice(&mytermfreqs[0..len]);
                } else {
                    termfreqs.iter_mut().take(len).for_each(|t| *t = 0);
                }
            }
        }
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl SkipListBlock {
    pub fn new(skip_list_format: &SkipListFormat) -> Self {
        let termfreqs = if skip_list_format.has_tflist() {
            Some(
                std::iter::repeat(0)
                    .take(SKIPLIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            len: 0,
            docids: [0; SKIPLIST_BLOCK_LEN],
            offsets: [0; SKIPLIST_BLOCK_LEN],
            termfreqs,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<A: Allocator> BuildingSkipList<A> {
    pub fn new(skip_list_format: SkipListFormat, slice_list: Arc<ByteSliceList<A>>) -> Self {
        Self {
            building_block: BuildingSkipListBlock::new(&skip_list_format),
            flushed_size: AcqRelUsize::new(0),
            slice_list,
            skip_list_format,
        }
    }
    pub fn building_block(&self) -> &BuildingSkipListBlock {
        &self.building_block
    }

    pub fn flushed_size(&self) -> usize {
        self.flushed_size.load()
    }

    pub fn slice_list(&self) -> &ByteSliceList<A> {
        &self.slice_list
    }

    pub fn skip_list_format(&self) -> &SkipListFormat {
        &self.skip_list_format
    }
}

impl<A: Allocator> BuildingSkipListWriter<A> {
    pub fn new(
        skip_list_format: SkipListFormat,
        initial_slice_capacity: usize,
        allocator: A,
    ) -> Self {
        let slice_writer =
            ByteSliceWriter::with_initial_capacity_in(initial_slice_capacity, allocator);
        let slice_list = slice_writer.slice_list();
        let building_skip_list =
            Arc::new(BuildingSkipList::new(skip_list_format.clone(), slice_list));

        Self {
            last_docid: INVALID_DOCID,
            last_offset: 0,
            block_len: 0,
            flushed_size: 0,
            slice_writer,
            skip_list_format,
            building_skip_list,
        }
    }

    pub fn building_skip_list(&self) -> Arc<BuildingSkipList<A>> {
        self.building_skip_list.clone()
    }

    pub fn add_skip_item(&mut self, last_docid: DocId, offset: usize, tf: Option<TermFreq>) {
        if self.last_docid == INVALID_DOCID {
            self.last_docid = 0;
        } else {
            assert!(last_docid > self.last_docid);
        }
        let building_block = &self.building_skip_list.building_block;
        building_block.add_skip_item(last_docid - self.last_docid, offset, self.block_len, tf);

        self.block_len += 1;
        building_block.len.store(self.block_len);
        if self.block_len == SKIPLIST_BLOCK_LEN {
            self.flush_building_block();
        }

        self.last_docid = last_docid;
        self.last_offset = offset;
    }

    fn flush_building_block(&mut self) {
        let building_block = &self.building_skip_list.building_block;
        let slice_writer = &mut self.slice_writer;
        let mut flushed_size = 1;
        slice_writer.write(self.block_len as u8);
        let docids = building_block.docids[0..self.block_len]
            .iter()
            .map(|a| a.load())
            .collect::<Vec<_>>();
        flushed_size += compression::copy_write(&docids, slice_writer);
        let offsets = building_block.offsets[0..self.block_len]
            .iter()
            .map(|a| a.load())
            .collect::<Vec<_>>();
        flushed_size += compression::copy_write(&offsets, slice_writer);
        if self.skip_list_format.has_tflist() {
            if let Some(termfreqs_atomics) = &building_block.termfreqs {
                let termfreqs = termfreqs_atomics[0..self.block_len]
                    .iter()
                    .map(|a| a.load())
                    .collect::<Vec<_>>();
                flushed_size += compression::copy_write(&termfreqs, slice_writer);
            }
        }

        self.flushed_size += flushed_size;
        self.building_skip_list
            .flushed_size
            .store(self.flushed_size);

        building_block.clear();
        self.block_len = 0;
    }
}

impl<'a> BuildingSkipListReader<'a> {
    pub fn open<A: Allocator>(building_skip_list: &'a BuildingSkipList<A>) -> Self {
        let slice_list = building_skip_list.slice_list();
        let mut flushed_size = building_skip_list.flushed_size();
        let mut slice_reader = ByteSliceReader::open(slice_list);
        let skip_list_format = building_skip_list.skip_list_format();
        let building_block = building_skip_list.building_block();
        let block_len = building_block.len();
        let mut block_snapshot = SkipListBlockSnapshot::with_capacity(block_len, skip_list_format);
        block_snapshot.snapshot(building_block, block_len);
        let flushed_size_updated = building_skip_list.flushed_size();
        if flushed_size < flushed_size_updated {
            block_snapshot.clear();
            flushed_size = flushed_size_updated;
            slice_reader = ByteSliceReader::open(slice_list);
        }
        let skip_list_format = building_skip_list.skip_list_format().clone();
        let skip_list_block = SkipListBlock::new(&skip_list_format);

        let empty = flushed_size == 0 && block_snapshot.is_empty();

        Self {
            empty,
            decoded: false,
            current_docid: 0,
            current_offset: 0,
            current_cursor: 0,
            skip_list_block,
            block_snapshot,
            flushed_size,
            slice_reader,
            skip_list_format,
        }
    }

    pub fn lookup(&mut self, query_docid: DocId) -> (DocId, usize, Option<TermFreq>) {
        if self.empty {
            return (0, 0, None);
        }

        loop {
            if self.current_cursor >= self.skip_list_block.len {
                if self.decoded {
                    break;
                }
                self.decode_one_block();
                if self.skip_list_block.is_empty() {
                    break;
                }
            }
            if self.current_docid + self.skip_list_block.docids[self.current_cursor] >= query_docid
            {
                break;
            }
            self.current_docid += self.skip_list_block.docids[self.current_cursor];
            self.current_offset += self.skip_list_block.offsets[self.current_cursor];
            self.current_cursor += 1;
        }

        (self.current_docid, self.current_offset, None)
    }

    fn decode_one_block(&mut self) {
        if self.slice_reader.tell() < self.flushed_size {
            self.decode_one_flushed_block();
        } else {
            self.decode_building_block();
        }
        self.current_cursor = 0;
    }

    fn decode_one_flushed_block(&mut self) {
        let block_len = self.slice_reader.read::<u8>() as usize;
        if block_len == SKIPLIST_BLOCK_LEN {
            self.skip_list_block.len = SKIPLIST_BLOCK_LEN;
            compression::copy_read(&mut self.slice_reader, &mut self.skip_list_block.docids);
            compression::copy_read(&mut self.slice_reader, &mut self.skip_list_block.offsets);
            if self.skip_list_format.has_tflist() {
                if let Some(termfreqs) = self.skip_list_block.termfreqs.as_deref_mut() {
                    compression::copy_read(&mut self.slice_reader, termfreqs);
                } else {
                    assert!(false);
                }
            }
        } else {
            self.skip_list_block.len = block_len;
            compression::copy_read(
                &mut self.slice_reader,
                &mut self.skip_list_block.docids[0..block_len],
            );
            compression::copy_read(
                &mut self.slice_reader,
                &mut self.skip_list_block.offsets[0..block_len],
            );
            if self.skip_list_format.has_tflist() {
                if let Some(termfreqs) = self.skip_list_block.termfreqs.as_deref_mut() {
                    compression::copy_read(&mut self.slice_reader, &mut termfreqs[0..block_len]);
                } else {
                    assert!(false);
                }
            }
        }
    }

    fn decode_building_block(&mut self) {
        self.block_snapshot.copy_to(&mut self.skip_list_block);
        self.decoded = true;
    }
}

#[cfg(test)]
mod tests {
    use allocator_api2::alloc::Global;

    use crate::{
        postings::skiplist::skip_list_format::SkipListFormatBuilder, DocId, SKIPLIST_BLOCK_LEN,
    };

    use super::{BuildingSkipListReader, BuildingSkipListWriter};

    #[test]
    fn test_basic() {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormatBuilder::default().with_tflist(false).build();
        let mut skip_list_writer =
            BuildingSkipListWriter::new(skip_list_format.clone(), 512, Global);
        let building_skip_list = skip_list_writer.building_skip_list();
        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        let (docid, offset, _) = skip_list_reader.lookup(0);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);

        skip_list_writer.add_skip_item(1000, 100, None);

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        let (docid, offset, _) = skip_list_reader.lookup(0);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1000);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1001);
        assert_eq!(docid, 1000);
        assert_eq!(offset, 100);

        skip_list_writer.add_skip_item(2000, 100, None);

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        let (docid, offset, _) = skip_list_reader.lookup(0);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1000);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1001);
        assert_eq!(docid, 1000);
        assert_eq!(offset, 100);
        let (docid, offset, _) = skip_list_reader.lookup(2000);
        assert_eq!(docid, 1000);
        assert_eq!(offset, 100);
        let (docid, offset, _) = skip_list_reader.lookup(2001);
        assert_eq!(docid, 2000);
        assert_eq!(offset, 200);

        skip_list_writer.add_skip_item(3000, 100, None);

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        let (docid, offset, _) = skip_list_reader.lookup(0);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1000);
        assert_eq!(docid, 0);
        assert_eq!(offset, 0);
        let (docid, offset, _) = skip_list_reader.lookup(1001);
        assert_eq!(docid, 1000);
        assert_eq!(offset, 100);
        let (docid, offset, _) = skip_list_reader.lookup(2000);
        assert_eq!(docid, 1000);
        assert_eq!(offset, 100);
        let (docid, offset, _) = skip_list_reader.lookup(2001);
        assert_eq!(docid, 2000);
        assert_eq!(offset, 200);
        let (docid, offset, _) = skip_list_reader.lookup(3000);
        assert_eq!(docid, 2000);
        assert_eq!(offset, 200);
        let (docid, offset, _) = skip_list_reader.lookup(3001);
        assert_eq!(docid, 3000);
        assert_eq!(offset, 300);

        for i in 3..BLOCK_LEN {
            skip_list_writer.add_skip_item(((i + 1) * 1000) as DocId, 100, None);
        }

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);

        let (docid, offset, _) = skip_list_reader.lookup(2001);
        assert_eq!(docid, 2000);
        assert_eq!(offset, 200);

        let (docid, offset, _) = skip_list_reader.lookup((BLOCK_LEN * 1000) as DocId);
        assert_eq!(docid, ((BLOCK_LEN - 1) * 1000) as DocId);
        assert_eq!(offset, 100 * (BLOCK_LEN - 1));

        let (docid, offset, _) = skip_list_reader.lookup((BLOCK_LEN * 1000 + 1) as DocId);
        assert_eq!(docid, (BLOCK_LEN * 1000) as DocId);
        assert_eq!(offset, 100 * BLOCK_LEN);

        skip_list_writer.add_skip_item(((BLOCK_LEN + 1) * 1000) as DocId, 100, None);

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);

        let (docid, offset, _) = skip_list_reader.lookup(2001);
        assert_eq!(docid, 2000);
        assert_eq!(offset, 200);

        let (docid, offset, _) = skip_list_reader.lookup((BLOCK_LEN * 1000) as DocId);
        assert_eq!(docid, ((BLOCK_LEN - 1) * 1000) as DocId);
        assert_eq!(offset, 100 * (BLOCK_LEN - 1));

        let (docid, offset, _) = skip_list_reader.lookup((BLOCK_LEN * 1000 + 1) as DocId);
        assert_eq!(docid, (BLOCK_LEN * 1000) as DocId);
        assert_eq!(offset, 100 * BLOCK_LEN);

        let (docid, offset, _) = skip_list_reader.lookup(((BLOCK_LEN + 1) * 1000) as DocId);
        assert_eq!(docid, (BLOCK_LEN * 1000) as DocId);
        assert_eq!(offset, 100 * BLOCK_LEN);

        let (docid, offset, _) = skip_list_reader.lookup(((BLOCK_LEN + 1) * 1000 + 1) as DocId);
        assert_eq!(docid, ((BLOCK_LEN + 1) * 1000) as DocId);
        assert_eq!(offset, 100 * (BLOCK_LEN + 1));
    }
}
