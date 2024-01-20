use std::{io, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use crate::postings::{ByteSliceList, ByteSliceReader, ByteSliceWriter};

use super::{
    BuildingSkipListBlock, SkipListBlockSnapshot, SkipListFlushInfo, SkipListFormat, SkipListRead,
    SkipListReader, SkipListWrite, SkipListWriter,
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
    building_skip_list: BuildingSkipList<A>,
}

pub struct BuildingSkipListReader<'a> {
    flushed_read_finished: bool,
    skipped_item_count: usize,
    current_key: u64,
    current_offset: u64,
    prev_value: u64,
    current_value: u64,
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
        let skip_list_writer = SkipListWriter::new(skip_list_format.clone(), byte_slice_writer);
        let building_skip_list = BuildingSkipList {
            building_block: skip_list_writer.building_block().clone(),
            flush_info: skip_list_writer.flush_info().clone(),
            byte_slice_list,
            skip_list_format,
        };

        Self {
            skip_list_writer,
            building_skip_list,
        }
    }

    pub fn building_skip_list(&self) -> &BuildingSkipList<A> {
        &self.building_skip_list
    }
}

impl<A: Allocator> SkipListWrite for BuildingSkipListWriter<A> {
    fn add_skip_item(&mut self, key: u64, offset: u64, value: Option<u64>) -> io::Result<()> {
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
        let mut byte_slice_reader = if item_count == 0 {
            ByteSliceReader::empty()
        } else {
            ByteSliceReader::open(byte_slice_list)
        };
        let mut building_block_snapshot = building_block.snapshot(flush_info.buffer_len());
        let item_count_updated = building_skip_list.flush_info.load().flushed_count();
        if item_count < item_count_updated {
            building_block_snapshot.clear();
            item_count = item_count_updated;
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        }
        let skip_list_reader =
            SkipListReader::open(skip_list_format, item_count, byte_slice_reader);

        Self {
            flushed_read_finished: false,
            skipped_item_count: 0,
            current_key: 0,
            current_offset: 0,
            prev_value: 0,
            current_value: 0,
            current_cursor: 0,
            building_block_snapshot,
            skip_list_reader,
        }
    }
}

impl<'a> SkipListRead for BuildingSkipListReader<'a> {
    fn seek(&mut self, key: u64) -> io::Result<(bool, u64, u64, u64, u64, usize)> {
        if !self.flushed_read_finished {
            let (ok, prev_key, block_last_key, start_offset, end_offset, skipped_item_count) =
                self.skip_list_reader.seek(key)?;
            if ok {
                return Ok((
                    true,
                    prev_key,
                    block_last_key,
                    start_offset,
                    end_offset,
                    skipped_item_count,
                ));
            }
            self.flushed_read_finished = true;
            self.current_key = prev_key;
            self.current_offset = start_offset;
            self.prev_value = self.skip_list_reader.prev_value();
            self.current_value = self.skip_list_reader.prev_value();
            self.skipped_item_count = skipped_item_count;
        }

        while self.current_cursor < self.building_block_snapshot.len() {
            let prev_key = self.current_key;
            let current_offset = self.current_offset;
            self.prev_value = self.current_value;
            let skipped_count = self.skipped_item_count;

            self.current_key +=
                self.building_block_snapshot.keys.as_ref().unwrap()[self.current_cursor] as u64;
            self.current_offset += self.building_block_snapshot.offsets[self.current_cursor] as u64;
            self.current_value +=
                self.building_block_snapshot
                    .values
                    .as_ref()
                    .map_or(0, |values1| values1[self.current_cursor]) as u64;
            self.skipped_item_count += 1;
            self.current_cursor += 1;

            if self.current_key >= key {
                return Ok((
                    true,
                    prev_key,
                    self.current_key,
                    current_offset,
                    self.current_offset,
                    skipped_count,
                ));
            }
        }

        Ok((
            false,
            self.current_key,
            self.current_key,
            self.current_offset,
            self.current_offset,
            self.skipped_item_count,
        ))
    }

    fn prev_value(&self) -> u64 {
        self.prev_value
    }

    fn block_last_value(&self) -> u64 {
        self.current_value
    }
}

#[cfg(test)]
mod tests {
    use std::{io, thread};

    use crate::{
        postings::skip_list::{
            BuildingSkipListReader, BuildingSkipListWriter, SkipListFormat, SkipListRead,
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
        let building_skip_list = skip_list_writer.building_skip_list().clone();
        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        assert_eq!(skip_list_reader.building_block_snapshot.len(), 0);
        let (found, prev_key, block_last_key, start_offset, end_offset, skipped_count) =
            skip_list_reader.seek(0)?;
        assert!(!found);
        assert_eq!(prev_key, 0);
        assert_eq!(block_last_key, 0);
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 0);
        assert_eq!(skipped_count, 0);

        skip_list_writer.add_skip_item(1000, 10, None)?;

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);

        assert_eq!(skip_list_reader.building_block_snapshot.len(), 1);
        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            skip_list_reader.seek(0)?;
        assert!(found);
        assert_eq!(prev_key, 0);
        assert_eq!(block_last_key, 1000);
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 10);
        assert_eq!(skipped, 0);

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);

        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            skip_list_reader.seek(0)?;
        assert!(found);
        assert_eq!(prev_key, 0);
        assert_eq!(block_last_key, 1000);
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 10);
        assert_eq!(skipped, 0);

        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            skip_list_reader.seek(1001)?;
        assert!(!found);
        assert_eq!(prev_key, 1000);
        assert_eq!(block_last_key, 1000);
        assert_eq!(start_offset, 10);
        assert_eq!(end_offset, 10);
        assert_eq!(skipped, 1);

        for i in 1..BLOCK_LEN {
            skip_list_writer.add_skip_item(((i + 1) * 1000) as u64, ((i + 1) * 10) as u64, None)?;
        }

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        assert_eq!(skip_list_reader.building_block_snapshot.len(), 0);

        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            skip_list_reader.seek(0)?;
        assert!(found);
        assert_eq!(prev_key, 0);
        assert_eq!(block_last_key, 1000);
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 10);
        assert_eq!(skipped, 0);

        for i in 1..BLOCK_LEN {
            let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
                skip_list_reader.seek(((i + 1) * 1000) as u64)?;
            assert!(found);
            assert_eq!(prev_key, (i * 1000) as u64);
            assert_eq!(block_last_key, ((i + 1) * 1000) as u64);
            assert_eq!(start_offset, (i * 10) as u64);
            assert_eq!(end_offset, ((i + 1) * 10) as u64);
            assert_eq!(skipped, i);
        }

        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            skip_list_reader.seek((BLOCK_LEN * 1000 + 1) as u64)?;
        assert!(!found);
        assert_eq!(prev_key, (BLOCK_LEN * 1000) as u64);
        assert_eq!(block_last_key, (BLOCK_LEN * 1000) as u64);
        assert_eq!(start_offset, (BLOCK_LEN * 10) as u64);
        assert_eq!(end_offset, (BLOCK_LEN * 10) as u64);
        assert_eq!(skipped, BLOCK_LEN);

        for i in 0..3 {
            skip_list_writer.add_skip_item(
                ((BLOCK_LEN + i + 1) * 1000) as u64,
                ((BLOCK_LEN + i + 1) * 10) as u64,
                None,
            )?;
        }

        let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
        assert_eq!(skip_list_reader.building_block_snapshot.len(), 3);

        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            skip_list_reader.seek(1000)?;
        assert!(found);
        assert_eq!(prev_key, 0);
        assert_eq!(block_last_key, 1000);
        assert_eq!(start_offset, 0);
        assert_eq!(end_offset, 10);
        assert_eq!(skipped, 0);

        for i in 1..BLOCK_LEN + 3 {
            let (found, prev_key, block_last_key, start_offset, _end_offset, skipped) =
                skip_list_reader.seek(((i + 1) * 1000) as u64)?;
            assert!(found);
            assert_eq!(prev_key, (i * 1000) as u64);
            assert_eq!(block_last_key, ((i + 1) * 1000) as u64);
            assert_eq!(start_offset, (i * 10) as u64);
            assert_eq!(skipped, i);
        }

        assert_eq!(skip_list_reader.current_cursor, 3);

        let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
            skip_list_reader.seek(u64::MAX)?;
        assert!(!found);
        assert_eq!(prev_key, ((BLOCK_LEN + 3) * 1000) as u64);
        assert_eq!(block_last_key, ((BLOCK_LEN + 3) * 1000) as u64);
        assert_eq!(start_offset, ((BLOCK_LEN + 3) * 10) as u64);
        assert_eq!(end_offset, ((BLOCK_LEN + 3) * 10) as u64);
        assert_eq!(skipped, BLOCK_LEN + 3);

        Ok(())
    }

    #[test]
    fn test_multithread() {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormat::builder().build();
        let mut skip_list_writer: BuildingSkipListWriter =
            BuildingSkipListWriter::new(skip_list_format.clone(), 1024);
        let building_skip_list = skip_list_writer.building_skip_list().clone();
        let w = thread::spawn(move || {
            for i in 0..BLOCK_LEN + 3 {
                skip_list_writer
                    .add_skip_item(((i + 1) * 1000) as u64, ((i + 1) * 10) as u64, None)
                    .unwrap();
            }
        });
        let r = thread::spawn(move || loop {
            let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
            let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
                skip_list_reader
                    .seek(((BLOCK_LEN + 3) * 1000) as u64)
                    .unwrap();
            if found {
                assert_eq!(prev_key, ((BLOCK_LEN + 2) * 1000) as u64);
                assert_eq!(block_last_key, ((BLOCK_LEN + 3) * 1000) as u64);
                assert_eq!(start_offset, ((BLOCK_LEN + 2) * 10) as u64);
                assert_eq!(end_offset, ((BLOCK_LEN + 3) * 10) as u64);
                assert_eq!(skipped, BLOCK_LEN + 2);

                let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
                    skip_list_reader
                        .seek((((BLOCK_LEN + 3) * 1000) + 1) as u64)
                        .unwrap();
                assert!(!found);
                assert_eq!(prev_key, ((BLOCK_LEN + 3) * 1000) as u64);
                assert_eq!(block_last_key, ((BLOCK_LEN + 3) * 1000) as u64);
                assert_eq!(start_offset, ((BLOCK_LEN + 3) * 10) as u64);
                assert_eq!(end_offset, ((BLOCK_LEN + 3) * 10) as u64);
                assert_eq!(skipped, BLOCK_LEN + 3);

                let mut skip_list_reader = BuildingSkipListReader::open(&building_skip_list);
                for i in 0..BLOCK_LEN + 3 {
                    let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
                        skip_list_reader.seek(((i + 1) * 1000) as u64).unwrap();
                    assert!(found);
                    assert_eq!(prev_key, (i * 1000) as u64);
                    assert_eq!(block_last_key, ((i + 1) * 1000) as u64);
                    assert_eq!(start_offset, (i * 10) as u64);
                    assert_eq!(end_offset, ((i + 1) * 10) as u64);
                    assert_eq!(skipped, i);
                }

                let (found, prev_key, block_last_key, start_offset, end_offset, skipped) =
                    skip_list_reader
                        .seek(((BLOCK_LEN + 4) * 1000) as u64)
                        .unwrap();
                assert!(!found);
                assert_eq!(prev_key, ((BLOCK_LEN + 3) * 1000) as u64);
                assert_eq!(block_last_key, ((BLOCK_LEN + 3) * 1000) as u64);
                assert_eq!(start_offset, ((BLOCK_LEN + 3) * 10) as u64);
                assert_eq!(end_offset, ((BLOCK_LEN + 3) * 10) as u64);
                assert_eq!(skipped, BLOCK_LEN + 3);

                break;
            }
            thread::yield_now();
        });

        w.join().unwrap();
        r.join().unwrap();
    }
}
