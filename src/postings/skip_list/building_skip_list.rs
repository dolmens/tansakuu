use std::{io, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use crate::postings::{ByteSliceList, ByteSliceReader, ByteSliceWriter};

use super::{
    BuildingSkipListBlock, SkipListBlockSnapshot, SkipListFlushInfo, SkipListFormat,
    SkipListReader, SkipListSeek, SkipListWrite, SkipListWriter,
};

#[derive(Clone)]
pub struct BuildingSkipList<A: Allocator = Global> {
    building_block: Arc<BuildingSkipListBlock>,
    flush_info: Arc<SkipListFlushInfo>,
    byte_slice_list: Arc<ByteSliceList<A>>,
    skip_list_format: SkipListFormat,
}

pub struct BuildingSkipListWriter<A: Allocator = Global> {
    skip_list_writer: SkipListWriter<ByteSliceWriter<A>>,
    byte_slice_list: Arc<ByteSliceList<A>>,
}

pub struct BuildingSkipListReader<'a> {
    skip_count: usize,
    current_key: u32,
    current_offset: usize,
    current_cursor: usize,
    building_block_snapshot: SkipListBlockSnapshot,
    skip_list_reader: SkipListReader<ByteSliceReader<'a>>,
}

#[cfg(test)]
impl<A: Allocator + Default> BuildingSkipListWriter<A> {
    pub fn new(skip_list_format: SkipListFormat, initial_slice_capacity: usize) -> Self {
        Self::new_in(skip_list_format, initial_slice_capacity, A::default())
    }
}

impl<A: Allocator> BuildingSkipListWriter<A> {
    pub fn new_in(
        skip_list_format: SkipListFormat,
        initial_slice_capacity: usize,
        allocator: A,
    ) -> Self {
        let byte_slice_writer =
            ByteSliceWriter::with_initial_capacity_in(initial_slice_capacity, allocator);
        let byte_slice_list = byte_slice_writer.byte_slice_list();
        let skip_list_writer = SkipListWriter::new(skip_list_format, byte_slice_writer);

        Self {
            skip_list_writer,
            byte_slice_list,
        }
    }

    pub fn building_skip_list(&self) -> BuildingSkipList<A> {
        BuildingSkipList {
            building_block: self.skip_list_writer.building_block().clone(),
            flush_info: self.skip_list_writer.flush_info().clone(),
            byte_slice_list: self.byte_slice_list.clone(),
            skip_list_format: self.skip_list_writer.skip_list_format().clone(),
        }
    }
}

impl<A: Allocator> SkipListWrite for BuildingSkipListWriter<A> {
    fn add_skip_item(&mut self, key: u32, offset: u32, value: Option<u32>) -> io::Result<()> {
        self.skip_list_writer.add_skip_item(key, offset, value)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.skip_list_writer.flush()
    }
}

impl<'a> BuildingSkipListReader<'a> {
    pub fn open<A: Allocator>(building_skip_list: &'a BuildingSkipList<A>) -> Self {
        let flush_info = building_skip_list.flush_info.load();
        let byte_slice_list = building_skip_list.byte_slice_list.as_ref();
        let building_block = building_skip_list.building_block.as_ref();
        let skip_list_format = building_skip_list.skip_list_format.clone();
        let mut item_count = flush_info.flushed_count();
        let mut byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        let mut building_block_snapshot = building_block.snapshot(flush_info.buffer_len());
        let item_count_updated = building_skip_list.flush_info.flushed_count();
        if item_count < item_count_updated {
            building_block_snapshot.clear();
            item_count = item_count_updated;
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        }
        let skip_list_reader =
            SkipListReader::open(skip_list_format, item_count, byte_slice_reader);

        Self {
            skip_count: 0,
            current_key: 0,
            current_offset: 0,
            current_cursor: 0,
            building_block_snapshot,
            skip_list_reader,
        }
    }
    // pub fn eof(&self) -> bool {
    //     self.skip_list_reader.eof() && self.current_cursor == self.building_block_snapshot.len()
    // }

    // pub fn current_key(&self) -> u32 {
    //     if !self.skip_list_reader.eof() {
    //         self.skip_list_reader.current_key()
    //     } else {
    //         self.current_key
    //     }
    // }

    // pub fn current_offset(&self) -> usize {
    //     if !self.skip_list_reader.eof() {
    //         self.skip_list_reader.current_offset()
    //     } else {
    //         self.current_offset
    //     }
    // }
}

impl<'a> SkipListSeek for BuildingSkipListReader<'a> {
    fn seek(&mut self, key: u32) -> io::Result<(usize, u32, usize)> {
        if !self.skip_list_reader.eof() {
            let (offset, current_key, skip_count) = self.skip_list_reader.seek(key)?;
            if !self.skip_list_reader.eof() {
                return Ok((offset, current_key, skip_count));
            }
            self.skip_count = skip_count;
            self.current_offset = offset;
            self.current_key = current_key;
        }
        while self.current_cursor < self.building_block_snapshot.len()
            && self.current_key + self.building_block_snapshot.keys[self.current_cursor] < key
        {
            self.current_key += self.building_block_snapshot.keys[self.current_cursor];
            self.current_offset +=
                self.building_block_snapshot.offsets[self.current_cursor] as usize;
            self.current_cursor += 1;
            self.skip_count += 1;
        }

        Ok((self.current_offset, self.current_key, self.skip_count))
    }
}

#[cfg(test)]
mod tests {
    use std::{io, thread};

    use crate::{
        postings::skip_list::{
            BuildingSkipListReader, BuildingSkipListWriter, SkipListFormat, SkipListSeek,
            SkipListWrite,
        },
        SKIPLIST_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormat::builder().build();
        let mut skip_list_writer: BuildingSkipListWriter =
            BuildingSkipListWriter::new(skip_list_format.clone(), 1024);
        let building_skip_list = skip_list_writer.building_skip_list();
        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        assert_eq!(skip_list_reader.building_block_snapshot.len(), 0);
        let (offset, current_key, skipped) = skip_list_reader.seek(0)?;
        assert_eq!(offset, 0);
        assert_eq!(current_key, 0);
        assert_eq!(skipped, 0);

        skip_list_writer.add_skip_item(1000, 100, None)?;

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        assert_eq!(skip_list_reader.building_block_snapshot.len(), 1);
        let (offset, current_key, skipped) = skip_list_reader.seek(0)?;
        assert_eq!(offset, 0);
        assert_eq!(current_key, 0);
        assert_eq!(skipped, 0);
        let (offset, current_key, skipped) = skip_list_reader.seek(1000)?;
        assert_eq!(offset, 0);
        assert_eq!(current_key, 0);
        assert_eq!(skipped, 0);
        let (offset, current_key, skipped) = skip_list_reader.seek(1001)?;
        assert_eq!(offset, 100);
        assert_eq!(current_key, 1000);
        assert_eq!(skipped, 1);
        let (offset, current_key, skipped) = skip_list_reader.seek(1002)?;
        assert_eq!(offset, 100);
        assert_eq!(current_key, 1000);
        assert_eq!(skipped, 1);

        for i in 1..BLOCK_LEN {
            skip_list_writer.add_skip_item(((i + 1) * 1000) as u32, 100, None)?;
        }
        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        assert_eq!(skip_list_reader.building_block_snapshot.len(), 0);

        let (offset, current_key, skipped) = skip_list_reader.seek(1000)?;
        assert_eq!(offset, 0);
        assert_eq!(current_key, 0);
        assert_eq!(skipped, 0);

        for i in 1..BLOCK_LEN {
            let (offset, current_key, skipped) = skip_list_reader.seek(((i + 1) * 1000) as u32)?;
            assert_eq!(offset, i * 100);
            assert_eq!(current_key, (i * 1000) as u32);
            assert_eq!(skipped, i);
        }

        let (offset, current_key, skipped) =
            skip_list_reader.seek((BLOCK_LEN * 1000 + 1) as u32)?;
        assert_eq!(offset, BLOCK_LEN * 100);
        assert_eq!(current_key, (BLOCK_LEN * 1000) as u32);
        assert_eq!(skipped, BLOCK_LEN);

        let (offset, current_key, skipped) =
            skip_list_reader.seek((BLOCK_LEN * 1000 + 2) as u32)?;
        assert_eq!(offset, BLOCK_LEN * 100);
        assert_eq!(current_key, (BLOCK_LEN * 1000) as u32);
        assert_eq!(skipped, BLOCK_LEN);

        for i in 0..3 {
            skip_list_writer.add_skip_item(((BLOCK_LEN + i + 1) * 1000) as u32, 100, None)?;
        }

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        assert_eq!(skip_list_reader.building_block_snapshot.len(), 3);

        let (offset, current_key, skipped) = skip_list_reader.seek(1000)?;
        assert_eq!(offset, 0);
        assert_eq!(current_key, 0);
        assert_eq!(skipped, 0);

        for i in 1..BLOCK_LEN + 4 {
            let (offset, current_key, skipped) = skip_list_reader.seek(((i + 1) * 1000) as u32)?;
            assert_eq!(offset, i * 100);
            assert_eq!(current_key, (i * 1000) as u32);
            assert_eq!(skipped, i);
        }

        assert_eq!(skip_list_reader.current_cursor, 3);

        let (offset, current_key, skipped) = skip_list_reader.seek(u32::MAX)?;
        assert_eq!(offset, (BLOCK_LEN + 3) * 100);
        assert_eq!(current_key, ((BLOCK_LEN + 3) * 1000) as u32);
        assert_eq!(skipped, BLOCK_LEN + 3);

        Ok(())
    }

    #[test]
    fn test_multithread() {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormat::builder().build();
        let mut skip_list_writer: BuildingSkipListWriter =
            BuildingSkipListWriter::new(skip_list_format.clone(), 1024);
        let building_skip_list = skip_list_writer.building_skip_list();
        let w = thread::spawn(move || {
            for i in 0..BLOCK_LEN + 3 {
                skip_list_writer
                    .add_skip_item(((i + 1) * 1000) as u32, 100, None)
                    .unwrap();
            }
        });
        let r = thread::spawn(move || loop {
            let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
            let (offset, current_key, skipped) = skip_list_reader
                .seek(((BLOCK_LEN + 4) * 1000) as u32)
                .unwrap();
            if offset == (BLOCK_LEN + 3) * 100 {
                assert_eq!(current_key, ((BLOCK_LEN + 3) * 1000) as u32);
                assert_eq!(skipped, BLOCK_LEN + 3);
                let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
                for i in 0..BLOCK_LEN + 3 {
                    let (offset, current_key, skipped) =
                        skip_list_reader.seek(((i + 2) * 1000) as u32).unwrap();
                    assert_eq!(offset, (i + 1) * 100);
                    assert_eq!(current_key, ((i + 1) * 1000) as u32);
                    assert_eq!(skipped, i + 1);
                }
                let (offset, current_key, skipped) = skip_list_reader
                    .seek(((BLOCK_LEN + 5) * 1000) as u32)
                    .unwrap();
                assert_eq!(offset, (BLOCK_LEN + 3) * 100);
                assert_eq!(current_key, ((BLOCK_LEN + 3) * 1000) as u32);
                assert_eq!(skipped, BLOCK_LEN + 3);
                break;
            }
            thread::yield_now();
        });

        w.join().unwrap();
        r.join().unwrap();
    }
}
