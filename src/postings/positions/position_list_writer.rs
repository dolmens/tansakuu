use std::{
    io::{self, Write},
    sync::Arc,
};

use tantivy_common::CountingWriter;

use crate::{
    postings::{PostingEncoder, SkipListFormat, SkipListWriter},
    util::RelaxedU32,
    TokenPos, POSITION_BLOCK_LEN,
};

pub struct PositionListWriter<W: Write, SW: Write> {
    last_pos: TokenPos,
    buffer_len: usize,
    flushed_count: usize,
    building_block: Arc<BuildingPositionsBlock>,
    writer: CountingWriter<W>,
    skip_list_writer: SkipListWriter<SW>,
}

pub struct BuildingPositionsBlock {
    positions: [RelaxedU32; POSITION_BLOCK_LEN],
}

impl BuildingPositionsBlock {
    pub fn new() -> Self {
        const ZERO: RelaxedU32 = RelaxedU32::new(0);

        Self {
            positions: [ZERO; POSITION_BLOCK_LEN],
        }
    }
}

impl<W: Write, SW: Write> PositionListWriter<W, SW> {
    pub fn new(writer: W, skip_list_writer: SW) -> Self {
        let skip_list_writer = SkipListWriter::new(SkipListFormat::default(), skip_list_writer);
        Self {
            last_pos: 0,
            buffer_len: 0,
            flushed_count: 0,
            building_block: Arc::new(BuildingPositionsBlock::new()),
            writer: CountingWriter::wrap(writer),
            skip_list_writer,
        }
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer_len
    }

    pub fn flushed_count(&self) -> usize {
        self.flushed_count
    }

    pub fn flushed_size(&self) -> usize {
        self.writer.written_bytes() as usize
    }

    pub fn add_pos(&mut self, pos: TokenPos) {
        self.building_block.positions[self.buffer_len].store(pos - self.last_pos);
        self.last_pos = pos;
    }

    pub fn end_doc(&mut self) -> io::Result<()> {
        self.last_pos = 0;

        self.buffer_len += 1;
        // self.building_block.len
        if self.buffer_len == POSITION_BLOCK_LEN {}

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.buffer_len > 0 {
            let building_block = self.building_block.as_ref();
            let posting_encoder = PostingEncoder;
            let positions = building_block.positions[0..self.buffer_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            posting_encoder.encode_u32(&positions, &mut self.writer)?;
        }

        Ok(())
    }
}
