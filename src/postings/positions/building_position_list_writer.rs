use std::{io, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use crate::{
    postings::{ByteSliceList, ByteSliceWriter},
    util::AcqRelU64,
    TokenPos, POSITION_BLOCK_LEN,
};

use super::position_list_writer::{BuildingPositionsBlock, PositionListWriter};

pub struct BuildingPositionListWriter<A: Allocator = Global> {
    position_list_writer: PositionListWriter<ByteSliceWriter<A>, ByteSliceWriter<A>>,
    building_position_list: Arc<BuildingPositionList>,
}

pub struct BuildingPositionList {
    flush_info: PositionListFlushInfo,
    building_block: Arc<BuildingPositionsBlock>,
    byte_slice_list: Arc<ByteSliceList>,
}

#[derive(Default)]
pub struct PositionListFlushInfo {
    value: AcqRelU64,
}

#[derive(Default)]
pub struct PositionListFlushInfoSnapshot {
    value: u64,
}

impl<A: Allocator> BuildingPositionListWriter<A> {
    pub fn add_pos(&mut self, pos: TokenPos) {
        self.position_list_writer.add_pos(pos);
    }

    pub fn end_doc(&mut self) -> io::Result<()> {
        self.position_list_writer.end_doc()?;
        let buffer_len = self.position_list_writer.buffer_len();
        let flushed_count = self.position_list_writer.flushed_count();
        let flush_info = PositionListFlushInfoSnapshot::new(flushed_count, buffer_len);
        self.building_position_list.flush_info.store(flush_info);
        Ok(())
    }
}

impl PositionListFlushInfo {
    pub fn load(&self) -> PositionListFlushInfoSnapshot {
        PositionListFlushInfoSnapshot::with_value(self.value.load())
    }

    fn store(&self, flush_info: PositionListFlushInfoSnapshot) {
        self.value.store(flush_info.value);
    }

    pub fn flushed_count(&self) -> usize {
        self.load().flushed_count()
    }

    fn set_buffer_len(&self, buffer_len: usize) {
        let mut flush_info = self.load();
        flush_info.set_buffer_len(buffer_len);
        self.store(flush_info);
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
