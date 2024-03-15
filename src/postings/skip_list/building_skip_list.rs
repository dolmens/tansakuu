use std::{io, sync::Arc};

use crate::{
    postings::{ByteSliceList, ByteSliceReader, ByteSliceWriter},
    util::fractional_capacity_policy::FractionalChunkCapacityPolicy,
};

use super::{
    BuildingSkipListBlock, SkipListBlockSnapshot, SkipListFormat, SkipListRead, SkipListReader,
    SkipListWrite, SkipListWriter,
};

#[derive(Clone)]
pub struct BuildingSkipList {
    building_block: Arc<BuildingSkipListBlock>,
    byte_slice_list: Arc<ByteSliceList>,
    skip_list_format: SkipListFormat,
}

pub struct BuildingSkipListWriter {
    skip_list_writer: SkipListWriter<ByteSliceWriter<FractionalChunkCapacityPolicy>>,
    building_skip_list: BuildingSkipList,
}

pub struct BuildingSkipListReader<'a> {
    flushed_count: usize,
    read_count: usize,
    current_key: u64,
    current_offset: u64,
    prev_value: u64,
    current_value: u64,
    current_cursor: usize,
    building_block_snapshot: SkipListBlockSnapshot,
    skip_list_reader: SkipListReader<ByteSliceReader<'a>>,
}

impl BuildingSkipListWriter {
    pub fn new(skip_list_format: SkipListFormat) -> Self {
        let byte_slice_writer = ByteSliceWriter::new();
        let byte_slice_list = byte_slice_writer.byte_slice_list();
        let skip_list_writer = SkipListWriter::new(skip_list_format, byte_slice_writer);
        let building_skip_list = BuildingSkipList {
            building_block: skip_list_writer.building_block().clone(),
            byte_slice_list,
            skip_list_format,
        };

        Self {
            skip_list_writer,
            building_skip_list,
        }
    }

    pub fn building_skip_list(&self) -> &BuildingSkipList {
        &self.building_skip_list
    }
}

impl SkipListWrite for BuildingSkipListWriter {
    fn add_skip_item_with_value(&mut self, key: u64, offset: u64, value: u64) -> io::Result<()> {
        self.skip_list_writer
            .add_skip_item_with_value(key, offset, value)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.skip_list_writer.flush()
    }

    fn written_bytes(&self) -> usize {
        self.skip_list_writer.written_bytes()
    }
}

impl<'a> BuildingSkipListReader<'a> {
    pub fn open(building_skip_list: &'a BuildingSkipList) -> Self {
        let flush_info = building_skip_list.building_block.flush_info.load();
        let byte_slice_list = building_skip_list.byte_slice_list.as_ref();
        let building_block = building_skip_list.building_block.as_ref();
        let skip_list_format = building_skip_list.skip_list_format;
        let mut flushed_count = flush_info.flushed_count();
        let mut byte_slice_reader = if flushed_count == 0 {
            ByteSliceReader::empty()
        } else {
            ByteSliceReader::open(byte_slice_list)
        };
        let mut building_block_snapshot = building_block.snapshot(flush_info.buffer_len());
        let flushed_count_updated = building_skip_list
            .building_block
            .flush_info
            .load()
            .flushed_count();
        if flushed_count < flushed_count_updated {
            building_block_snapshot.clear();
            flushed_count = flushed_count_updated;
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        }
        let skip_list_reader =
            SkipListReader::open(skip_list_format, flushed_count, byte_slice_reader);

        Self {
            flushed_count,
            read_count: 0,
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
        if self.read_count < self.flushed_count {
            let (ok, prev_key, block_last_key, start_offset, end_offset, skipped_item_count) =
                self.skip_list_reader.seek(key)?;
            self.read_count = skipped_item_count;
            if ok {
                self.current_key = block_last_key;
                self.prev_value = self.skip_list_reader.prev_value();
                self.current_value = self.skip_list_reader.current_value();
                return Ok((
                    true,
                    prev_key,
                    block_last_key,
                    start_offset,
                    end_offset,
                    skipped_item_count,
                ));
            }
            self.current_key = prev_key;
            self.current_offset = start_offset;
            self.prev_value = self.skip_list_reader.prev_value();
            self.current_value = self.skip_list_reader.prev_value();
        }

        while self.current_cursor < self.building_block_snapshot.len() {
            let prev_key = self.current_key;
            let current_offset = self.current_offset;
            self.prev_value = self.current_value;
            let skipped_count = self.read_count;

            self.current_key +=
                self.building_block_snapshot.keys.as_ref()[self.current_cursor] as u64;
            self.current_offset += self.building_block_snapshot.offsets[self.current_cursor] as u64;
            self.current_value +=
                self.building_block_snapshot
                    .values
                    .as_ref()
                    .map_or(0, |values1| values1[self.current_cursor]) as u64;
            self.read_count += 1;
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
            self.read_count,
        ))
    }

    fn current_key(&self) -> u64 {
        self.current_key
    }

    fn prev_value(&self) -> u64 {
        self.prev_value
    }

    fn current_value(&self) -> u64 {
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
        let mut skip_list_writer = BuildingSkipListWriter::new(skip_list_format);
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

        skip_list_writer.add_skip_item(1000, 10)?;

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
            skip_list_writer.add_skip_item(((i + 1) * 1000) as u64, ((i + 1) * 10) as u64)?;
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
        let mut skip_list_writer = BuildingSkipListWriter::new(skip_list_format);
        let building_skip_list = skip_list_writer.building_skip_list().clone();
        let w = thread::spawn(move || {
            for i in 0..BLOCK_LEN + 3 {
                skip_list_writer
                    .add_skip_item(((i + 1) * 1000) as u64, ((i + 1) * 10) as u64)
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
