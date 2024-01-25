use std::{
    io::{self, Write},
    sync::Arc,
};

use tantivy_common::CountingWriter;

use crate::{
    postings::{
        skip_list::{SkipListFormat, SkipListWrite, SkipListWriter},
        PostingEncoder,
    },
    util::{AcqRelU64, RelaxedU32},
    POSITION_BLOCK_LEN,
};

use super::PositionListBlock;

pub trait PositionListWrite {
    fn add_pos(&mut self, pos: u32) -> io::Result<()>;
    fn end_doc(&mut self);
    fn flush(&mut self) -> io::Result<()>;
    fn item_count(&self) -> (usize, usize);
    fn written_bytes(&self) -> (usize, usize);
}

pub struct PositionListWriter<W: Write, S: SkipListWrite> {
    item_count: usize,
    last_pos: u32,
    buffer_len: usize,
    item_count_flushed: usize,
    flush_info: Arc<PositionListFlushInfo>,
    building_block: Arc<BuildingPositionListBlock>,
    output_writer: CountingWriter<W>,
    skip_list_writer: S,
}

pub struct PositionListWriterBuilder<W: Write, S: SkipListWrite> {
    output_writer: W,
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

pub struct EmptyPositionListWriter;

impl PositionListWrite for EmptyPositionListWriter {
    fn add_pos(&mut self, _pos: u32) -> io::Result<()> {
        Ok(())
    }
    fn end_doc(&mut self) {}
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn item_count(&self) -> (usize, usize) {
        (0, 0)
    }
    fn written_bytes(&self) -> (usize, usize) {
        (0, 0)
    }
}

pub fn none_position_list_writer() -> Option<EmptyPositionListWriter> {
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

impl<W: Write> PositionListWriterBuilder<W, SkipListWriter<io::Sink>> {
    pub fn new(output_writer: W) -> Self {
        let skip_list_writer = SkipListWriter::new(SkipListFormat::default(), io::sink());
        Self {
            output_writer,
            skip_list_writer,
        }
    }
}

impl<W: Write, S: SkipListWrite> PositionListWriterBuilder<W, S> {
    pub fn with_skip_list_output_writer<SW: Write>(
        self,
        skip_list_output_writer: SW,
    ) -> PositionListWriterBuilder<W, SkipListWriter<SW>> {
        let skip_list_writer =
            SkipListWriter::new(SkipListFormat::default(), skip_list_output_writer);
        PositionListWriterBuilder {
            output_writer: self.output_writer,
            skip_list_writer,
        }
    }

    pub fn with_skip_list_writer<SW: SkipListWrite>(
        self,
        skip_list_writer: SW,
    ) -> PositionListWriterBuilder<W, SW> {
        PositionListWriterBuilder {
            output_writer: self.output_writer,
            skip_list_writer,
        }
    }

    pub fn build(self) -> PositionListWriter<W, S> {
        PositionListWriter::new(self.output_writer, self.skip_list_writer)
    }
}

impl<W: Write, S: SkipListWrite> PositionListWriter<W, S> {
    pub fn new(output_writer: W, skip_list_writer: S) -> Self {
        let flush_info = Arc::new(PositionListFlushInfo::new());
        Self {
            item_count: 0,
            last_pos: 0,
            buffer_len: 0,
            item_count_flushed: 0,
            flush_info,
            building_block: Arc::new(BuildingPositionListBlock::new()),
            output_writer: CountingWriter::wrap(output_writer),
            skip_list_writer,
        }
    }

    pub fn flush_info(&self) -> &Arc<PositionListFlushInfo> {
        &self.flush_info
    }

    pub fn building_block(&self) -> &Arc<BuildingPositionListBlock> {
        &self.building_block
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer_len > 0 {
            let building_block = self.building_block.as_ref();
            let posting_encoder = PostingEncoder;
            let positions = building_block.positions[0..self.buffer_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            posting_encoder.encode_u32(&positions, &mut self.output_writer)?;

            self.item_count_flushed += self.buffer_len;
            self.buffer_len = 0;

            // The skip item is the block's last key
            self.skip_list_writer.add_skip_item(
                self.item_count_flushed as u64 - 1,
                self.output_writer.written_bytes(),
                None,
            )?;

            let flush_info = PositionListFlushInfoSnapshot::new(self.item_count_flushed, 0);
            self.flush_info.store(flush_info);
        }

        Ok(())
    }
}

impl<W: Write, S: SkipListWrite> PositionListWrite for PositionListWriter<W, S> {
    fn add_pos(&mut self, pos: u32) -> io::Result<()> {
        self.item_count += 1;
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

    fn end_doc(&mut self) {
        self.last_pos = 0;
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.skip_list_writer.flush()?;

        Ok(())
    }

    fn item_count(&self) -> (usize, usize) {
        (self.item_count, self.skip_list_writer.item_count())
    }

    fn written_bytes(&self) -> (usize, usize) {
        (
            self.output_writer.written_bytes() as usize,
            self.skip_list_writer.written_bytes(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            positions::PositionListWrite, positions::PositionListWriter,
            skip_list::MockSkipListWriter, PostingEncoder,
        },
        POSITION_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_BLOCK_LEN;

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
        let mut skip_list_keys = vec![];
        let mut skip_list_offsets = vec![];
        let mut skip_list_values = vec![];
        let skip_list_writer = MockSkipListWriter::new(
            &mut skip_list_keys,
            &mut skip_list_offsets,
            Some(&mut skip_list_values),
        );
        let mut position_list_writer = PositionListWriter::new(&mut output_buf, skip_list_writer);
        position_list_writer.add_pos(positions[0])?;
        position_list_writer.add_pos(positions[1])?;
        position_list_writer.end_doc();

        for i in 2..BLOCK_LEN {
            position_list_writer.add_pos(positions[i])?;
        }
        position_list_writer.end_doc();

        position_list_writer.add_pos(positions[BLOCK_LEN])?;
        position_list_writer.add_pos(positions[BLOCK_LEN + 1])?;
        position_list_writer.end_doc();

        for i in BLOCK_LEN + 2..BLOCK_LEN * 2 + 3 {
            position_list_writer.add_pos(positions[i])?;
        }
        position_list_writer.end_doc();

        position_list_writer.flush()?;

        assert_eq!(skip_list_keys.len(), 3);
        assert_eq!(skip_list_keys[0] as usize, BLOCK_LEN - 1);
        assert_eq!(skip_list_keys[1] as usize, BLOCK_LEN * 2 - 1);
        assert_eq!(skip_list_keys[2] as usize, BLOCK_LEN * 2 + 3 - 1);

        let posting_encoder = PostingEncoder;

        let mut buf_reader = BufReader::new(output_buf.as_slice());
        let mut position_block = [0u32; POSITION_BLOCK_LEN];
        let mut num_read_bytes =
            posting_encoder.decode_u32(&mut buf_reader, &mut position_block[..])?;
        assert_eq!(num_read_bytes, skip_list_offsets[0] as usize);
        assert_eq!(position_block, &positions_deltas[0..BLOCK_LEN]);

        num_read_bytes += posting_encoder.decode_u32(&mut buf_reader, &mut position_block[..])?;
        assert_eq!(num_read_bytes, skip_list_offsets[1] as usize);
        assert_eq!(position_block, &positions_deltas[BLOCK_LEN..BLOCK_LEN * 2]);

        num_read_bytes += posting_encoder.decode_u32(&mut buf_reader, &mut position_block[0..3])?;
        assert_eq!(num_read_bytes, skip_list_offsets[2] as usize);
        assert_eq!(
            &position_block[0..3],
            &positions_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        Ok(())
    }
}
