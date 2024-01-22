use std::io::{self, Read, Seek, SeekFrom};

use crate::{
    postings::{
        positions::PositionListBlock, skip_list::SkipListRead, PostingEncoder, SkipListFormat,
        SkipListReader,
    },
    POSITION_BLOCK_LEN,
};

pub struct PositionListReader<R: Read + Seek, S: SkipListRead> {
    read_count: usize,
    item_count: usize,
    reader: R,
    skip_list_reader: S,
}

pub trait PositionListRead {
    fn decode_one_block(
        &mut self,
        ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool>;
}

pub struct EmptyPositionListRead;
impl PositionListRead for EmptyPositionListRead {
    fn decode_one_block(
        &mut self,
        _from_ttf: u64,
        _position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        Ok(false)
    }
}

pub fn none_position_list_reader() -> Option<EmptyPositionListRead> {
    None
}

impl<R: Read + Seek, SR: Read> PositionListReader<R, SkipListReader<SR>> {
    pub fn open(
        item_count: usize,
        reader: R,
        skip_list_item_count: usize,
        skip_list_reader: SR,
    ) -> Self {
        let skip_list_format = SkipListFormat::default();
        let skip_list_reader =
            SkipListReader::open(skip_list_format, skip_list_item_count, skip_list_reader);
        Self::open_with_skip_list_reader(item_count, reader, skip_list_reader)
    }
}

impl<R: Read + Seek, S: SkipListRead> PositionListReader<R, S> {
    pub fn open_with_skip_list_reader(item_count: usize, reader: R, skip_list_reader: S) -> Self {
        Self {
            read_count: 0,
            item_count,
            reader,
            skip_list_reader,
        }
    }
}

impl<R: Read + Seek, S: SkipListRead> PositionListRead for PositionListReader<R, S> {
    fn decode_one_block(
        &mut self,
        ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        if self.read_count == self.item_count
            || (ttf as usize) >= self.item_count
            || (ttf as usize) < self.read_count
        {
            return Ok(false);
        }
        let (_skip_found, _prev_key, _block_last_key, start_offset, _end_offset, skipped_count) =
            self.skip_list_reader.seek(ttf)?;
        self.read_count = skipped_count * POSITION_BLOCK_LEN;

        self.reader.seek(SeekFrom::Start(start_offset))?;

        let block_len = std::cmp::min(self.item_count - self.read_count, POSITION_BLOCK_LEN);
        position_list_block.len = block_len;
        position_list_block.start_ttf = self.read_count as u64;
        let posting_encoder = PostingEncoder;
        posting_encoder.decode_u32(
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
            positions::{PositionListBlock, PositionListRead, PositionListReader},
            skip_list::MockSkipListReader,
            PostingEncoder,
        },
        POSITION_BLOCK_LEN,
    };

    #[test]
    fn test_short_list() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_BLOCK_LEN;
        let positions: Vec<_> = (0..(BLOCK_LEN - 1) as u32).collect();
        let mut buf = vec![];
        let posting_encoder = PostingEncoder;
        posting_encoder.encode_u32(&positions, &mut buf)?;

        {
            let buf_reader = Cursor::new(buf.as_slice());

            let skipbuf = vec![];
            let mut position_list_reader =
                PositionListReader::open(BLOCK_LEN - 1, buf_reader, 0, &skipbuf[..]);

            let mut position_list_block = PositionListBlock::new();
            assert!(position_list_reader.decode_one_block(0, &mut position_list_block)?);
            assert_eq!(position_list_block.len, BLOCK_LEN - 1);
            assert_eq!(position_list_block.start_ttf, 0);
            assert_eq!(
                &position_list_block.positions[0..BLOCK_LEN - 1],
                &positions[..]
            );
            assert_eq!(position_list_reader.read_count, BLOCK_LEN - 1);
        }

        {
            let buf_reader = Cursor::new(buf.as_slice());

            let skipbuf = vec![];
            let mut position_list_reader =
                PositionListReader::open(BLOCK_LEN - 1, buf_reader, 0, &skipbuf[..]);

            let mut position_list_block = PositionListBlock::new();
            assert!(position_list_reader.decode_one_block(3, &mut position_list_block)?);
            assert_eq!(position_list_block.len, BLOCK_LEN - 1);
            assert_eq!(position_list_block.start_ttf, 0);
            assert_eq!(
                &position_list_block.positions[0..BLOCK_LEN - 1],
                &positions[..]
            );
            assert_eq!(position_list_reader.read_count, BLOCK_LEN - 1);
        }

        {
            let buf_reader = Cursor::new(buf.as_slice());

            let skipbuf = vec![];
            let mut position_list_reader =
                PositionListReader::open(BLOCK_LEN - 1, buf_reader, 0, &skipbuf[..]);

            let mut position_list_block = PositionListBlock::new();
            assert!(!position_list_reader
                .decode_one_block((BLOCK_LEN - 1) as u64, &mut position_list_block)?);
        }

        Ok(())
    }

    #[test]
    fn test_with_skip_list() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_BLOCK_LEN;
        let positions: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as u32).collect();

        let mut buf = vec![];
        let keys: Vec<u64> = vec![(BLOCK_LEN - 1) as u64, (BLOCK_LEN * 2 - 1) as u64];
        let mut offsets = Vec::<u64>::new();
        let mut offset = 0;

        let posting_encoder = PostingEncoder;
        offset += posting_encoder.encode_u32(&positions[0..BLOCK_LEN], &mut buf)?;
        offsets.push(offset as u64);
        offset += posting_encoder.encode_u32(&positions[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)?;
        offsets.push(offset as u64);
        posting_encoder.encode_u32(&positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)?;

        let mut position_list_block = PositionListBlock::new();

        let skip_list_reader = MockSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_reader = PositionListReader::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_reader.decode_one_block(0, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, 0);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[0..BLOCK_LEN]
        );

        let skip_list_reader = MockSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_reader = PositionListReader::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_reader.decode_one_block(BLOCK_LEN as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[BLOCK_LEN..BLOCK_LEN * 2]
        );

        assert!(position_list_reader
            .decode_one_block((BLOCK_LEN * 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert_eq!(position_list_reader.read_count, BLOCK_LEN * 2 + 3);

        // read from middle

        let skip_list_reader = MockSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_reader = PositionListReader::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_reader.decode_one_block(3, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, 0);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[0..BLOCK_LEN]
        );

        assert!(position_list_reader
            .decode_one_block((BLOCK_LEN + 5) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[BLOCK_LEN..BLOCK_LEN * 2]
        );

        assert!(position_list_reader
            .decode_one_block((BLOCK_LEN * 2 + 1) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        // skip one block

        let skip_list_reader = MockSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_reader = PositionListReader::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_reader
            .decode_one_block((BLOCK_LEN + 3) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, BLOCK_LEN);
        assert_eq!(position_list_block.start_ttf, BLOCK_LEN as u64);
        assert_eq!(
            &position_list_block.positions[0..BLOCK_LEN],
            &positions[BLOCK_LEN..BLOCK_LEN * 2]
        );
        assert_eq!(position_list_reader.read_count, BLOCK_LEN * 2);

        assert!(position_list_reader
            .decode_one_block((BLOCK_LEN * 2 + 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        // skip two block

        let skip_list_reader = MockSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_reader = PositionListReader::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_reader
            .decode_one_block((BLOCK_LEN * 2 + 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(!position_list_reader
            .decode_one_block((BLOCK_LEN * 2 + 3) as u64, &mut position_list_block)?);

        Ok(())
    }

    #[test]
    fn test_with_skip_list_last_block_not_full() -> io::Result<()> {
        const BLOCK_LEN: usize = POSITION_BLOCK_LEN;
        let positions: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as u32).collect();

        let mut buf = vec![];
        let keys: Vec<u64> = vec![
            (BLOCK_LEN - 1) as u64,
            (BLOCK_LEN * 2 - 1) as u64,
            (BLOCK_LEN * 2 + 3 - 1) as u64,
        ];
        let mut offsets = Vec::<u64>::new();
        let mut offset = 0;

        let posting_encoder = PostingEncoder;
        offset += posting_encoder.encode_u32(&positions[0..BLOCK_LEN], &mut buf)?;
        offsets.push(offset as u64);
        offset += posting_encoder.encode_u32(&positions[BLOCK_LEN..BLOCK_LEN * 2], &mut buf)?;
        offsets.push(offset as u64);
        offset +=
            posting_encoder.encode_u32(&positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3], &mut buf)?;
        offsets.push(offset as u64);

        let mut position_list_block = PositionListBlock::new();

        let skip_list_reader = MockSkipListReader::new(keys.clone(), offsets.clone(), None);
        let buf_reader = Cursor::new(buf.as_slice());
        let mut position_list_reader = PositionListReader::open_with_skip_list_reader(
            BLOCK_LEN * 2 + 3,
            buf_reader,
            skip_list_reader,
        );

        assert!(position_list_reader
            .decode_one_block((BLOCK_LEN * 2 + 2) as u64, &mut position_list_block)?);
        assert_eq!(position_list_block.len, 3);
        assert_eq!(position_list_block.start_ttf, (BLOCK_LEN * 2) as u64);
        assert_eq!(
            &position_list_block.positions[0..3],
            &positions[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        assert!(!position_list_reader
            .decode_one_block((BLOCK_LEN * 2 + 3) as u64, &mut position_list_block)?);

        Ok(())
    }
}
