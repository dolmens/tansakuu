use std::{io, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use crate::{
    postings::{
        skip_list::{
            AtomicBuildingSkipList, BuildingSkipListReader, DeferredBuildingSkipListWriter,
            SkipListFormat,
        },
        ByteSliceList, ByteSliceReader, ByteSliceWriter,
    },
    util::fractional_capacity_policy::FractionalChunkCapacityPolicy,
    MAX_UNCOMPRESSED_POSITION_LIST_LEN,
};

use super::{
    BuildingPositionListBlock, PositionListBlock, PositionListBlockSnapshot, PositionListDecode,
    PositionListDecoder, PositionListEncode, PositionListEncoder,
};

#[derive(Clone)]
pub struct BuildingPositionList<A: Allocator = Global> {
    pub building_block: Arc<BuildingPositionListBlock>,
    pub byte_slice_list: Arc<ByteSliceList<A>>,
    pub building_skip_list: Arc<AtomicBuildingSkipList<A>>,
}

pub struct BuildingPositionListEncoder<A: Allocator + Clone = Global> {
    position_list_encoder: PositionListEncoder<
        ByteSliceWriter<FractionalChunkCapacityPolicy, A>,
        DeferredBuildingSkipListWriter<A>,
    >,
    building_position_list: BuildingPositionList<A>,
}

pub struct BuildingPositionListDecoder<'a> {
    read_count: usize,
    flushed_count: usize,
    buffer_len: usize,
    building_block_snapshot: PositionListBlockSnapshot,
    position_list_decoder: PositionListDecoder<ByteSliceReader<'a>, BuildingSkipListReader<'a>>,
}

impl<A: Allocator + Clone + Default> BuildingPositionListEncoder<A> {
    pub fn new() -> Self {
        Self::new_in(Default::default())
    }
}

impl<A: Allocator + Clone> BuildingPositionListEncoder<A> {
    pub fn new_in(allocator: A) -> Self {
        let byte_slice_writer = ByteSliceWriter::new_in(allocator.clone());
        let byte_slice_list = byte_slice_writer.byte_slice_list();
        let skip_list_format = SkipListFormat::builder().build();
        let skip_list_writer =
            DeferredBuildingSkipListWriter::new_in(skip_list_format, allocator.clone());
        let building_skip_list = skip_list_writer.building_skip_list();
        let position_list_encoder = PositionListEncoder::new(byte_slice_writer, skip_list_writer);
        let building_block = position_list_encoder.building_block().clone();
        let building_position_list = BuildingPositionList {
            building_block,
            byte_slice_list,
            building_skip_list,
        };

        Self {
            position_list_encoder,
            building_position_list,
        }
    }

    pub fn building_position_list(&self) -> &BuildingPositionList<A> {
        &self.building_position_list
    }
}

impl<A: Allocator + Clone> PositionListEncode for BuildingPositionListEncoder<A> {
    fn add_pos(&mut self, pos: u32) -> io::Result<()> {
        self.position_list_encoder.add_pos(pos)
    }

    fn end_doc(&mut self) {
        self.position_list_encoder.end_doc();
    }

    fn flush(&mut self) -> io::Result<()> {
        self.position_list_encoder.flush()
    }

    fn ttf(&self) -> usize {
        self.position_list_encoder.ttf()
    }

    fn written_bytes(&self) -> (usize, usize) {
        self.position_list_encoder.written_bytes()
    }
}

impl<'a> BuildingPositionListDecoder<'a> {
    pub fn open<A: Allocator>(building_position_list: &'a BuildingPositionList<A>) -> Self {
        let byte_slice_list = building_position_list.byte_slice_list.as_ref();
        let building_block = building_position_list.building_block.as_ref();
        let flush_info = building_position_list.building_block.flush_info.load();
        let mut flushed_count = flush_info.flushed_count();
        let mut buffer_len = flush_info.buffer_len();
        let mut byte_slice_reader = if flushed_count == 0 {
            ByteSliceReader::empty()
        } else {
            ByteSliceReader::open(byte_slice_list)
        };
        let mut building_block_snapshot = building_block.snapshot(buffer_len);
        let flushed_count_updated = building_position_list
            .building_block
            .flush_info
            .load()
            .flushed_count();
        if flushed_count < flushed_count_updated {
            building_block_snapshot.clear();
            flushed_count = flushed_count_updated;
            buffer_len = 0;
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        }

        let skip_list_reader = if flushed_count > MAX_UNCOMPRESSED_POSITION_LIST_LEN {
            Some(BuildingSkipListReader::open(
                building_position_list.building_skip_list.load(),
            ))
        } else {
            None
        };

        let position_list_decoder = PositionListDecoder::open_with_skip_list_reader_optional(
            flushed_count,
            byte_slice_reader,
            skip_list_reader,
        );

        Self {
            read_count: 0,
            flushed_count,
            buffer_len,
            building_block_snapshot,
            position_list_decoder,
        }
    }
}

impl<'a> PositionListDecode for BuildingPositionListDecoder<'a> {
    fn decode_position_buffer(
        &mut self,
        ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if (ttf as usize) >= self.flushed_count + self.buffer_len {
            return Ok(false);
        }
        if (ttf as usize) < self.flushed_count {
            self.position_list_decoder
                .decode_position_buffer(ttf, position_list_block)?;
            self.read_count += position_list_block.len;
            Ok(true)
        } else {
            self.building_block_snapshot.copy_to(position_list_block);
            position_list_block.start_ttf = self.flushed_count as u64;
            self.read_count += self.buffer_len;
            Ok(true)
        }
    }

    fn decode_next_record(
        &mut self,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if self.read_count >= self.flushed_count + self.buffer_len {
            return Ok(false);
        }

        if self.read_count < self.flushed_count {
            self.position_list_decoder
                .decode_next_record(position_list_block)?;
            self.read_count += position_list_block.len;
            Ok(true)
        } else {
            self.building_block_snapshot.copy_to(position_list_block);
            position_list_block.start_ttf = self.flushed_count as u64;
            self.read_count += self.buffer_len;
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, thread};

    use crate::{
        postings::positions::{
            building_position_list::BuildingPositionListDecoder, BuildingPositionListEncoder,
            PositionListBlock, PositionListDecode, PositionListEncode,
        },
        POSITION_LIST_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_LIST_BLOCK_LEN;
        let mut position_list_encoder: BuildingPositionListEncoder =
            BuildingPositionListEncoder::new();
        let building_position_list = position_list_encoder.building_position_list().clone();
        let mut position_list_block = PositionListBlock::new();
        let position_list_decoder = BuildingPositionListDecoder::open(&building_position_list);
        assert_eq!(position_list_decoder.flushed_count, 0);
        assert_eq!(position_list_decoder.buffer_len, 0);

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

        position_list_encoder.add_pos(positions[0])?;
        position_list_encoder.add_pos(positions[1])?;
        position_list_encoder.end_doc();

        let mut position_list_decoder = BuildingPositionListDecoder::open(&building_position_list);
        assert!(position_list_decoder.decode_position_buffer(0, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 2);
        assert_eq!(position_list_block.start_ttf, 0);
        assert_eq!(
            &position_list_block.positions[0..2],
            &positions_deltas[0..2]
        );
        assert!(!position_list_decoder.decode_position_buffer(2, &mut position_list_block)?);

        for i in 2..BLOCK_LEN {
            position_list_encoder.add_pos(positions[i])?;
        }
        position_list_encoder.end_doc();

        let mut position_list_decoder = BuildingPositionListDecoder::open(&building_position_list);
        assert!(position_list_decoder.decode_position_buffer(0, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, 0);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions_deltas[0..BLOCK_LEN]
        );
        assert!(!position_list_decoder
            .decode_position_buffer(BLOCK_LEN as u64, &mut position_list_block)?);

        position_list_encoder.add_pos(positions[BLOCK_LEN])?;
        position_list_encoder.add_pos(positions[BLOCK_LEN + 1])?;
        position_list_encoder.end_doc();

        for i in BLOCK_LEN + 2..BLOCK_LEN * 2 + 3 {
            position_list_encoder.add_pos(positions[i])?;
        }
        position_list_encoder.end_doc();

        let mut position_list_decoder = BuildingPositionListDecoder::open(&building_position_list);

        assert!(position_list_decoder.decode_position_buffer(0, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, 0);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions_deltas[0..BLOCK_LEN]
        );

        assert!(position_list_decoder
            .decode_position_buffer(BLOCK_LEN as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions_deltas[BLOCK_LEN..BLOCK_LEN * 2]
        );

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert!(!position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 3) as u64, &mut position_list_block)?);

        // skip one block

        let mut position_list_decoder = BuildingPositionListDecoder::open(&building_position_list);

        assert!(position_list_decoder
            .decode_position_buffer(BLOCK_LEN as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions_deltas[BLOCK_LEN..BLOCK_LEN * 2]
        );

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert!(!position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 3) as u64, &mut position_list_block)?);

        // skip two block

        let mut position_list_decoder = BuildingPositionListDecoder::open(&building_position_list);

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert!(!position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 3) as u64, &mut position_list_block)?);

        Ok(())
    }

    #[test]
    fn test_multi_thread() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_LIST_BLOCK_LEN;
        let mut position_list_encoder: BuildingPositionListEncoder =
            BuildingPositionListEncoder::new();
        let building_position_list = position_list_encoder.building_position_list().clone();

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

        thread::scope(|scope| {
            let writer = scope.spawn(move || -> io::Result<()> {
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

                Ok(())
            });

            let reader = scope.spawn(|| -> io::Result<()> {
                let mut position_list_block = PositionListBlock::new();
                loop {
                    let mut position_list_decoder =
                        BuildingPositionListDecoder::open(&building_position_list);
                    let mut ttf = 0_usize;
                    while position_list_decoder
                        .decode_position_buffer(ttf as u64, &mut position_list_block)?
                    {
                        let len = position_list_block.len;
                        assert_eq!(position_list_block.start_ttf, ttf as u64);
                        assert_eq!(
                            &position_list_block.positions[0..len],
                            &positions_deltas[ttf..ttf + len]
                        );
                        ttf += len;
                    }
                    if ttf >= BLOCK_LEN * 2 + 3 {
                        break;
                    }
                    thread::yield_now();
                }

                Ok(())
            });

            writer.join().unwrap()?;
            reader.join().unwrap()?;

            Ok(())
        })
    }
}
