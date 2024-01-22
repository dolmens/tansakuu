use std::{
    io::{self, Write},
    sync::Arc,
};

use tantivy_common::CountingWriter;

use crate::{
    postings::{skip_list::SkipListWrite, PostingEncoder, SkipListFormat, SkipListWriter},
    util::{AcqRelU64, RelaxedU32},
    POSITION_BLOCK_LEN,
};

use super::PositionListBlock;

pub struct PositionListWriter<W: Write, S: SkipListWrite> {
    last_pos: u32,
    buffer_len: usize,
    item_count_flushed: usize,
    flush_info: Arc<PositionListFlushInfo>,
    building_block: Arc<BuildingPositionListBlock>,
    writer: CountingWriter<W>,
    skip_list_writer: S,
}

#[derive(Default)]
pub struct PositionListFlushInfo {
    value: AcqRelU64,
}

#[derive(Default)]
pub struct PositionListFlushInfoSnapshot {
    value: u64,
}

pub struct BuildingPositionListBlock {
    positions: [RelaxedU32; POSITION_BLOCK_LEN],
}

#[derive(Default)]
pub struct PositionListBlockSnapshot {
    len: usize,
    positions: Option<Box<[u32]>>,
}

impl PositionListFlushInfo {
    pub fn new() -> Self {
        Self {
            value: AcqRelU64::new(0),
        }
    }

    pub fn load(&self) -> PositionListFlushInfoSnapshot {
        PositionListFlushInfoSnapshot::with_value(self.value.load())
    }

    fn store(&self, flush_info: PositionListFlushInfoSnapshot) {
        self.value.store(flush_info.value);
    }
}

impl PositionListFlushInfoSnapshot {
    const BUFFER_LEN_MASK: u64 = 0xFFFF_FFFF;
    const FLUSHED_COUNT_MASK: u64 = 0xFFFF_FFFF_0000_0000;

    pub fn new(flushed_count: usize, buffer_len: usize) -> Self {
        let value = ((flushed_count as u64) << 32) | ((buffer_len as u64) & Self::BUFFER_LEN_MASK);
        Self { value }
    }

    pub fn with_value(value: u64) -> Self {
        Self { value }
    }

    pub fn buffer_len(&self) -> usize {
        (self.value & Self::BUFFER_LEN_MASK) as usize
    }

    pub fn set_buffer_len(&mut self, buffer_len: usize) {
        self.value =
            (self.value & Self::FLUSHED_COUNT_MASK) | ((buffer_len as u64) & Self::BUFFER_LEN_MASK);
    }

    pub fn flushed_count(&self) -> usize {
        (self.value >> 32) as usize
    }

    pub fn set_flushed_count(&mut self, flushed_count: usize) {
        self.value = (self.value & Self::BUFFER_LEN_MASK) | ((flushed_count as u64) << 32);
    }
}

impl BuildingPositionListBlock {
    pub fn new() -> Self {
        const ZERO: RelaxedU32 = RelaxedU32::new(0);

        Self {
            positions: [ZERO; POSITION_BLOCK_LEN],
        }
    }

    pub fn snapshot(&self, len: usize) -> PositionListBlockSnapshot {
        if len > 0 {
            let positions = self.positions[0..len]
                .iter()
                .map(|pos| pos.load())
                .collect();
            PositionListBlockSnapshot {
                len,
                positions: Some(positions),
            }
        } else {
            PositionListBlockSnapshot::default()
        }
    }
}

impl PositionListBlockSnapshot {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn copy_to(&self, position_list_block: &mut PositionListBlock) {
        position_list_block.len = self.len;
        if self.len > 0 {
            position_list_block.positions[0..self.len]
                .copy_from_slice(&self.positions.as_ref().unwrap()[0..self.len]);
        }
    }
}

impl<W: Write, S: SkipListWrite> PositionListWriter<W, S> {
    pub fn new_with_skip_list_writer(writer: W, skip_list_writer: S) -> Self {
        let flush_info = Arc::new(PositionListFlushInfo::new());
        Self {
            last_pos: 0,
            buffer_len: 0,
            item_count_flushed: 0,
            flush_info,
            building_block: Arc::new(BuildingPositionListBlock::new()),
            writer: CountingWriter::wrap(writer),
            skip_list_writer,
        }
    }

    pub fn flush_info(&self) -> &Arc<PositionListFlushInfo> {
        &self.flush_info
    }

    pub fn building_block(&self) -> &Arc<BuildingPositionListBlock> {
        &self.building_block
    }

    pub fn add_pos(&mut self, pos: u32) -> io::Result<()> {
        self.building_block.positions[self.buffer_len].store(pos - self.last_pos);
        self.buffer_len += 1;
        let flush_info =
            PositionListFlushInfoSnapshot::new(self.item_count_flushed, self.buffer_len);
        self.flush_info.store(flush_info);

        if self.buffer_len == POSITION_BLOCK_LEN {
            self.flush_buffer()?;
        }

        self.last_pos = pos;

        Ok(())
    }

    pub fn end_doc(&mut self) {
        self.last_pos = 0;
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer_len > 0 {
            let building_block = self.building_block.as_ref();
            let posting_encoder = PostingEncoder;
            let positions = building_block.positions[0..self.buffer_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            posting_encoder.encode_u32(&positions, &mut self.writer)?;

            self.item_count_flushed += self.buffer_len;
            self.buffer_len = 0;

            // The skip item is the block's last key
            self.skip_list_writer.add_skip_item(
                self.item_count_flushed as u64 - 1,
                self.writer.written_bytes(),
                None,
            )?;

            let flush_info = PositionListFlushInfoSnapshot::new(self.item_count_flushed, 0);
            self.flush_info.store(flush_info);
        }

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.skip_list_writer.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::POSITION_BLOCK_LEN;

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_BLOCK_LEN;

        // let positions:Vec<_> = 0.
        Ok(())
    }
}
