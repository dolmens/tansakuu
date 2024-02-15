use std::io::{self, Read, Seek, SeekFrom};

use crate::{
    postings::{
        compression::BlockEncoder,
        positions::PositionListBlock,
        skip_list::{SkipListFormat, SkipListRead, SkipListReader},
    },
    MAX_UNCOMPRESSED_POSITION_LIST_LEN, POSITION_LIST_BLOCK_LEN,
};

pub trait PositionListDecode {
    fn decode_position_buffer(
        &mut self,
        ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool>;

    fn decode_next_record(
        &mut self,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool>;
}

pub struct PositionListDecoder<R: Read + Seek, S: SkipListRead> {
    read_count: usize,
    ttf: usize,
    reader: R,
    skip_list_reader: Option<S>,
}

pub struct EmptyPositionListDecoder;
impl PositionListDecode for EmptyPositionListDecoder {
    fn decode_position_buffer(
        &mut self,
        _from_ttf: u64,
        _position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        Ok(false)
    }

    fn decode_next_record(
        &mut self,
        _position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        Ok(false)
    }
}

pub fn none_position_list_decoder() -> Option<EmptyPositionListDecoder> {
    None
}

impl<R: Read + Seek, SR: Read> PositionListDecoder<R, SkipListReader<SR>> {
    pub fn open(ttf: usize, reader: R, skip_list_reader: SR) -> Self {
        let skip_list_format = SkipListFormat::default();
        let skip_list_reader = if ttf > MAX_UNCOMPRESSED_POSITION_LIST_LEN {
            let skip_list_item_count =
                (ttf + POSITION_LIST_BLOCK_LEN - 1) / POSITION_LIST_BLOCK_LEN;
            Some(SkipListReader::open(
                skip_list_format,
                skip_list_item_count,
                skip_list_reader,
            ))
        } else {
            None
        };

        Self {
            read_count: 0,
            ttf,
            reader,
            skip_list_reader,
        }
    }
}

impl<R: Read + Seek, S: SkipListRead> PositionListDecoder<R, S> {
    pub fn open_with_skip_list_reader(ttf: usize, reader: R, skip_list_reader: S) -> Self {
        Self {
            read_count: 0,
            ttf,
            reader,
            skip_list_reader: Some(skip_list_reader),
        }
    }

    pub fn open_with_skip_list_reader_optional(
        ttf: usize,
        reader: R,
        skip_list_reader: Option<S>,
    ) -> Self {
        Self {
            read_count: 0,
            ttf,
            reader,
            skip_list_reader,
        }
    }

    fn decode_position_buffer_short_list(
        &mut self,
        ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if self.read_count == self.ttf || (ttf as usize) >= self.ttf {
            return Ok(false);
        }

        let block_len = self.ttf;
        position_list_block.len = block_len;
        position_list_block.start_ttf = 0;
        let block_encoder = BlockEncoder;
        block_encoder.decode_u32(
            &mut self.reader,
            &mut position_list_block.positions[0..block_len],
        )?;

        self.read_count += block_len;

        Ok(true)
    }
}

impl<R: Read + Seek, S: SkipListRead> PositionListDecode for PositionListDecoder<R, S> {
    fn decode_position_buffer(
        &mut self,
        ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if self.ttf <= MAX_UNCOMPRESSED_POSITION_LIST_LEN {
            return self.decode_position_buffer_short_list(ttf, position_list_block);
        }

        if self.read_count == self.ttf || (ttf as usize) >= self.ttf {
            return Ok(false);
        }

        let skip_list_reader = self.skip_list_reader.as_mut().unwrap();
        let (skip_found, _prev_key, _block_last_key, start_offset, _end_offset, skipped_count) =
            skip_list_reader.seek(ttf)?;
        if !skip_found {
            return Ok(false);
        }
        self.read_count = skipped_count * POSITION_LIST_BLOCK_LEN;

        self.reader.seek(SeekFrom::Start(start_offset))?;

        let block_len = std::cmp::min(self.ttf - self.read_count, POSITION_LIST_BLOCK_LEN);
        position_list_block.len = block_len;
        position_list_block.start_ttf = self.read_count as u64;
        let block_encoder = BlockEncoder;
        block_encoder.decode_u32(
            &mut self.reader,
            &mut position_list_block.positions[0..block_len],
        )?;

        self.read_count += block_len;

        Ok(true)
    }

    fn decode_next_record(
        &mut self,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if self.read_count == self.ttf {
            return Ok(false);
        }

        let block_len = std::cmp::min(self.ttf - self.read_count, POSITION_LIST_BLOCK_LEN);
        position_list_block.len = block_len;
        position_list_block.start_ttf = self.read_count as u64;
        let block_encoder = BlockEncoder;
        block_encoder.decode_u32(
            &mut self.reader,
            &mut position_list_block.positions[0..block_len],
        )?;

        self.read_count += block_len;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use crate::{
        postings::{
            compression::BlockEncoder,
            positions::{PositionListBlock, PositionListDecode, PositionListDecoder},
            skip_list::BasicSkipListReader,
        },
        MAX_UNCOMPRESSED_POSITION_LIST_LEN, POSITION_LIST_BLOCK_LEN,
    };

    #[test]
    fn test_short_list() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_LIST_BLOCK_LEN;
        const UNCOMPRESSED_LEN: usize = MAX_UNCOMPRESSED_POSITION_LIST_LEN;
        let positions: Vec<_> = (0..UNCOMPRESSED_LEN as u32).collect();
        let mut buf = vec![];
        let block_encoder = BlockEncoder;
        block_encoder.encode_u32(&positions, &mut buf)?;

        {
            let buf_reader = Cursor::new(buf.as_slice());

            let skipbuf = vec![];
            let mut position_list_decoder =
                PositionListDecoder::open(UNCOMPRESSED_LEN, buf_reader, &skipbuf[..]);

            let mut position_list_block = PositionListBlock::new();
            assert!(position_list_decoder.decode_position_buffer(0, &mut position_list_block)?);
            assert_eq!(position_list_block.len, UNCOMPRESSED_LEN);
            assert_eq!(position_list_block.start_ttf, 0);
            assert_eq!(
                &position_list_block.positions[0..UNCOMPRESSED_LEN],
                &positions[..]
            );
            assert_eq!(position_list_decoder.read_count, UNCOMPRESSED_LEN);
        }

        {
            let buf_reader = Cursor::new(buf.as_slice());

            let skipbuf = vec![];
            let mut position_list_decoder =
                PositionListDecoder::open(UNCOMPRESSED_LEN, buf_reader, &skipbuf[..]);

            let mut position_list_block = PositionListBlock::new();
            assert!(position_list_decoder
                .decode_position_buffer((UNCOMPRESSED_LEN - 1) as u64, &mut position_list_block)?);
            assert_eq!(position_list_block.len, UNCOMPRESSED_LEN);
            assert_eq!(position_list_block.start_ttf, 0);
            assert_eq!(
                &position_list_block.positions[0..UNCOMPRESSED_LEN],
                &positions[..]
            );
            assert_eq!(position_list_decoder.read_count, UNCOMPRESSED_LEN);
        }

        {
            let buf_reader = Cursor::new(buf.as_slice());

            let skipbuf = vec![];
            let mut position_list_decoder =
                PositionListDecoder::open(UNCOMPRESSED_LEN, buf_reader, &skipbuf[..]);

            let mut position_list_block = PositionListBlock::new();
            assert!(!position_list_decoder
                .decode_position_buffer(UNCOMPRESSED_LEN as u64, &mut position_list_block)?);
        }

        Ok(())
    }

    #[test]
    fn test_with_skip_list() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_LIST_BLOCK_LEN;
        let positions: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as u32).collect();

        let mut buf = vec![];
        let keys: Vec<u64> = vec![
            (BLOCK_LEN - 1) as u64,
            (BLOCK_LEN * 2 - 1) as u64,
            (BLOCK_LEN * 2 + 3 - 1) as u64,
        ];
        let mut offsets = Vec::<u64>::new();
        let mut offset = 0;

        let block_encoder = BlockEncoder;
        offset += block_encoder.encode_u32(&positions[0..BLOCK_LEN], &mut buf)?;
        offsets.push(offset as u64);
        offset += block_encoder.encode_u32(&positions[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)?;
        offsets.push(offset as u64);
        offset +=
            block_encoder.encode_u32(&positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)?;
        offsets.push(offset as u64);

        let mut position_list_block = PositionListBlock::new();

        let skip_list_reader = BasicSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_decoder = PositionListDecoder::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_decoder.decode_position_buffer(0, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, 0);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[0..BLOCK_LEN]
        );

        let skip_list_reader = BasicSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_decoder = PositionListDecoder::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_decoder
            .decode_position_buffer(BLOCK_LEN as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[BLOCK_LEN..BLOCK_LEN * 2]
        );

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert_eq!(position_list_decoder.read_count, BLOCK_LEN * 2 + 3);

        // read from middle

        let skip_list_reader = BasicSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_decoder = PositionListDecoder::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_decoder.decode_position_buffer(3, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, 0);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[0..BLOCK_LEN]
        );

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN + 5) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[BLOCK_LEN..BLOCK_LEN * 2]
        );

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 1) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        // skip one block

        let skip_list_reader = BasicSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_decoder = PositionListDecoder::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN + 3) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[BLOCK_LEN..BLOCK_LEN * 2]
        );
        assert_eq!(position_list_decoder.read_count, BLOCK_LEN * 2);

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        // skip two block

        let skip_list_reader = BasicSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_decoder = PositionListDecoder::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(!position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 3) as u64, &mut position_list_block)?);

        Ok(())
    }

    #[test]
    fn test_with_skip_list_last_block_not_full() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_LIST_BLOCK_LEN;
        let positions: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as u32).collect();

        let mut buf = vec![];
        let keys: Vec<u64> = vec![
            (BLOCK_LEN - 1) as u64,
            (BLOCK_LEN * 2 - 1) as u64,
            (BLOCK_LEN * 2 + 3 - 1) as u64,
        ];
        let mut offsets = Vec::<u64>::new();
        let mut offset = 0;

        let block_encoder = BlockEncoder;
        offset += block_encoder.encode_u32(&positions[0..BLOCK_LEN], &mut buf)?;
        offsets.push(offset as u64);
        offset += block_encoder.encode_u32(&positions[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)?;
        offsets.push(offset as u64);
        offset +=
            block_encoder.encode_u32(&positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)?;
        offsets.push(offset as u64);

        let mut position_list_block = PositionListBlock::new();

        let skip_list_reader = BasicSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_decoder = PositionListDecoder::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(!position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2 + 3) as u64, &mut position_list_block)?);

        Ok(())
    }

    #[test]
    fn test_decode_next_record() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_LIST_BLOCK_LEN;
        let positions: Vec<_> = (0..(BLOCK_LEN * 3 + 3) as u32).collect();

        let mut buf = vec![];
        let keys: Vec<u64> = vec![
            (BLOCK_LEN - 1) as u64,
            (BLOCK_LEN * 2 - 1) as u64,
            (BLOCK_LEN * 3 - 1) as u64,
        ];
        let mut offsets = Vec::<u64>::new();
        let mut offset = 0;

        let block_encoder = BlockEncoder;
        offset += block_encoder.encode_u32(&positions[0..BLOCK_LEN], &mut buf)?;
        offsets.push(offset as u64);
        offset += block_encoder.encode_u32(&positions[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)?;
        offsets.push(offset as u64);
        block_encoder.encode_u32(&positions[BLOCK_LEN * 2..BLOCK_LEN * 3], &mut buf)?;
        offsets.push(offset as u64);
        block_encoder.encode_u32(&positions[BLOCK_LEN * 3..BLOCK_LEN * 3 + 3], &mut buf)?;

        let mut position_list_block = PositionListBlock::new();

        let skip_list_reader = BasicSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_decoder = PositionListDecoder::open_with_skip_list_reader(
            BLOCK_LEN * 3 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_decoder.decode_position_buffer(0, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, 0);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[0..BLOCK_LEN]
        );

        assert!(position_list_decoder.decode_next_record(&mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[BLOCK_LEN..BLOCK_LEN * 2]
        );

        // skip again
        assert!(position_list_decoder
            .decode_position_buffer((BLOCK_LEN * 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 3]
        );

        // next gain
        assert!(position_list_decoder.decode_next_record(&mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 3) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 3..BLOCK_LEN * 3 + 3]
        );

        Ok(())
    }
}
