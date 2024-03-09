use std::{
    io::{self, Write},
    sync::{Arc, atomic::{fence, Ordering}},
};

use tantivy_common::CountingWriter;

use crate::{
    postings::{
        compression::BlockEncoder,
        skip_list::{BasicSkipListWriter, SkipListFormat, SkipListWrite, SkipListWriter},
    },
    util::atomic::{AcqRelU64, RelaxedU32},
    MAX_UNCOMPRESSED_POSITION_LIST_LEN, POSITION_LIST_BLOCK_LEN,
};

use super::PositionListBlock;

pub trait PositionListEncode {
    fn add_pos(&mut self, pos: u32) -> io::Result<()>;
    fn end_doc(&mut self);
    fn flush(&mut self) -> io::Result<()>;
    fn ttf(&self) -> usize;
    fn written_bytes(&self) -> (usize, usize);
}

pub struct PositionListEncoder<W: Write, S: SkipListWrite> {
    ttf: usize,
    last_pos: u32,
    buffer_len: usize,
    item_count_flushed: usize,
    building_block: Arc<BuildingPositionListBlock>,
    writer: CountingWriter<W>,
    skip_list_writer: S,
}

pub struct PositionListEncoderBuilder<W: Write, S: SkipListWrite> {
    writer: W,
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
    pub flush_info: PositionListFlushInfo,
    positions: [RelaxedU32; POSITION_LIST_BLOCK_LEN],
}

#[derive(Default)]
pub struct PositionListBlockSnapshot {
    len: usize,
    positions: Option<Box<[u32]>>,
}

pub struct EmptyPositionListEncoder;

impl PositionListEncode for EmptyPositionListEncoder {
    fn add_pos(&mut self, _pos: u32) -> io::Result<()> {
        Ok(())
    }
    fn end_doc(&mut self) {}
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn ttf(&self) -> usize {
        0
    }
    fn written_bytes(&self) -> (usize, usize) {
        (0, 0)
    }
}

pub fn none_position_list_encoder() -> Option<EmptyPositionListEncoder> {
    None
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

    pub fn load_with_fence(&self) -> PositionListFlushInfoSnapshot {
        fence(Ordering::Acquire);
        PositionListFlushInfoSnapshot::with_value(self.value.load())
    }

    fn store(&self, flush_info: PositionListFlushInfoSnapshot) {
        self.value.store(flush_info.value);
        fence(Ordering::Release);
    }
}

impl PositionListFlushInfoSnapshot {
    const BUFFER_LEN_MASK: u64 = 0xFF;
    const FLUSHED_COUNT_MASK: u64 = 0xFFFF_FFFF_FFFF_FF00;

    pub fn new(flushed_count: usize, buffer_len: usize) -> Self {
        let value = ((flushed_count as u64) << 8) | ((buffer_len as u64) & Self::BUFFER_LEN_MASK);
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
        (self.value >> 8) as usize
    }

    pub fn set_flushed_count(&mut self, flushed_count: usize) {
        self.value = (self.value & Self::BUFFER_LEN_MASK) | ((flushed_count as u64) << 8);
    }
}

impl BuildingPositionListBlock {
    pub fn new() -> Self {
        const ZERO: RelaxedU32 = RelaxedU32::new(0);

        Self {
            flush_info: PositionListFlushInfo::new(),
            positions: [ZERO; POSITION_LIST_BLOCK_LEN],
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

pub fn position_list_encoder_builder() -> PositionListEncoderBuilder<io::Sink, BasicSkipListWriter>
{
    PositionListEncoderBuilder {
        writer: io::sink(),
        skip_list_writer: BasicSkipListWriter::default(),
    }
}

impl<W: Write, S: SkipListWrite> PositionListEncoderBuilder<W, S> {
    pub fn with_writer<OW: Write>(self, writer: OW) -> PositionListEncoderBuilder<OW, S> {
        PositionListEncoderBuilder {
            writer,
            skip_list_writer: self.skip_list_writer,
        }
    }

    pub fn with_skip_list_output_writer<SW: Write>(
        self,
        skip_list_output_writer: SW,
    ) -> PositionListEncoderBuilder<W, SkipListWriter<SW>> {
        let skip_list_writer =
            SkipListWriter::new(SkipListFormat::default(), skip_list_output_writer);
        PositionListEncoderBuilder {
            writer: self.writer,
            skip_list_writer,
        }
    }

    pub fn with_skip_list_writer<SW: SkipListWrite>(
        self,
        skip_list_writer: SW,
    ) -> PositionListEncoderBuilder<W, SW> {
        PositionListEncoderBuilder {
            writer: self.writer,
            skip_list_writer,
        }
    }

    pub fn build(self) -> PositionListEncoder<W, S> {
        PositionListEncoder::new(self.writer, self.skip_list_writer)
    }
}

impl<W: Write, S: SkipListWrite> PositionListEncoder<W, S> {
    pub fn new(writer: W, skip_list_writer: S) -> Self {
        Self {
            ttf: 0,
            last_pos: 0,
            buffer_len: 0,
            item_count_flushed: 0,
            building_block: Arc::new(BuildingPositionListBlock::new()),
            writer: CountingWriter::wrap(writer),
            skip_list_writer,
        }
    }

    pub fn skip_list_writer(&self) -> &S {
        &self.skip_list_writer
    }

    pub fn building_block(&self) -> &Arc<BuildingPositionListBlock> {
        &self.building_block
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer_len > 0 {
            let building_block = self.building_block.as_ref();
            let block_encoder = BlockEncoder;
            let positions = building_block.positions[0..self.buffer_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            block_encoder.encode_u32(&positions, &mut self.writer)?;

            self.item_count_flushed += self.buffer_len;
            self.buffer_len = 0;

            if self.ttf > MAX_UNCOMPRESSED_POSITION_LIST_LEN {
                // The skip item is the block's last key
                self.skip_list_writer.add_skip_item(
                    self.item_count_flushed as u64 - 1,
                    self.writer.written_bytes(),
                    None,
                )?;
            }

            let flush_info = PositionListFlushInfoSnapshot::new(self.item_count_flushed, 0);
            self.building_block.flush_info.store(flush_info);
        }

        Ok(())
    }
}

impl<W: Write, S: SkipListWrite> PositionListEncode for PositionListEncoder<W, S> {
    fn add_pos(&mut self, pos: u32) -> io::Result<()> {
        self.ttf += 1;
        self.building_block.positions[self.buffer_len].store(pos - self.last_pos);
        self.buffer_len += 1;
        let flush_info =
            PositionListFlushInfoSnapshot::new(self.item_count_flushed, self.buffer_len);
        self.building_block.flush_info.store(flush_info);

        if self.buffer_len == POSITION_LIST_BLOCK_LEN {
            self.flush_buffer()?;
        }

        self.last_pos = pos;

        Ok(())
    }

    fn end_doc(&mut self) {
        self.last_pos = 0;
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.skip_list_writer.flush()?;

        Ok(())
    }

    fn ttf(&self) -> usize {
        self.ttf
    }

    fn written_bytes(&self) -> (usize, usize) {
        (
            self.writer.written_bytes() as usize,
            self.skip_list_writer.written_bytes(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            compression::BlockEncoder, positions::PositionListEncode,
            positions::PositionListEncoder, skip_list::BasicSkipListWriter,
        },
        POSITION_LIST_BLOCK_LEN,
    };

    use super::PositionListFlushInfoSnapshot;

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_LIST_BLOCK_LEN;

        let positions: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as u32).collect();
        // the positions 0 and 1 are one doc, BLOCK_LEN and BLOCK_LEN + 1 are one doc.
        let mut positions_deltas = vec![positions[0], positions[1] - positions[0], positions[2]];
        positions_deltas.extend(
            positions[2..BLOCK_LEN]
                .windows(2)
                .map(|pair| pair[1] - pair[0]),
        );
        positions_deltas.push(positions[BLOCK_LEN]);
        positions_deltas.push(positions[BLOCK_LEN + 1] - positions[BLOCK_LEN]);
        positions_deltas.push(positions[BLOCK_LEN + 2]);
        positions_deltas.extend(
            positions[BLOCK_LEN + 2..]
                .windows(2)
                .map(|pair| pair[1] - pair[0]),
        );

        let mut output_buf = vec![];
        let skip_list_writer = BasicSkipListWriter::default();
        let mut position_list_encoder = PositionListEncoder::new(&mut output_buf, skip_list_writer);
        position_list_encoder.add_pos(positions[0])?;
        position_list_encoder.add_pos(positions[1])?;
        position_list_encoder.end_doc();

        for i in 2..BLOCK_LEN {
            position_list_encoder.add_pos(positions[i])?;
        }
        position_list_encoder.end_doc();

        position_list_encoder.add_pos(positions[BLOCK_LEN])?;
        position_list_encoder.add_pos(positions[BLOCK_LEN + 1])?;
        position_list_encoder.end_doc();

        for i in BLOCK_LEN + 2..BLOCK_LEN * 2 + 3 {
            position_list_encoder.add_pos(positions[i])?;
        }
        position_list_encoder.end_doc();

        position_list_encoder.flush()?;

        let skip_list_keys = position_list_encoder.skip_list_writer().keys.clone();
        let skip_list_offsets = position_list_encoder.skip_list_writer().offsets.clone();

        assert_eq!(skip_list_keys.len(), 3);
        assert_eq!(skip_list_keys[0] as usize, BLOCK_LEN - 1);
        assert_eq!(skip_list_keys[1] as usize, BLOCK_LEN * 2 - 1);
        assert_eq!(skip_list_keys[2] as usize, BLOCK_LEN * 2 + 3 - 1);

        let block_encoder = BlockEncoder;

        let mut buf_reader = BufReader::new(output_buf.as_slice());
        let mut position_block = [0u32; POSITION_LIST_BLOCK_LEN];
        let mut num_read_bytes =
            block_encoder.decode_u32(&mut buf_reader, &mut position_block[..])?;
        assert_eq!(num_read_bytes, skip_list_offsets[0] as usize);
        assert_eq!(position_block, &positions_deltas[0..BLOCK_LEN]);

        num_read_bytes += block_encoder.decode_u32(&mut buf_reader, &mut position_block[..])?;
        assert_eq!(num_read_bytes, skip_list_offsets[1] as usize);
        assert_eq!(position_block, &positions_deltas[BLOCK_LEN..BLOCK_LEN * 2]);

        num_read_bytes += block_encoder.decode_u32(&mut buf_reader, &mut position_block[0..3])?;
        assert_eq!(num_read_bytes, skip_list_offsets[2] as usize);
        assert_eq!(
            &position_block[0..3],
            &positions_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        Ok(())
    }

    #[test]
    fn test_position_flush_info() {
        let mut flush_info = PositionListFlushInfoSnapshot::new(100, 3);
        assert_eq!(flush_info.flushed_count(), 100);
        assert_eq!(flush_info.buffer_len(), 3);
        flush_info.set_buffer_len(128);
        assert_eq!(flush_info.flushed_count(), 100);
        assert_eq!(flush_info.buffer_len(), 128);
        let flushed_count: usize = 0x11_FFFF_FFFF_FFF1;
        flush_info.set_flushed_count(flushed_count);
        assert_eq!(flush_info.flushed_count(), flushed_count);
        assert_eq!(flush_info.buffer_len(), 128);
    }
}
