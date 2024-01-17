use std::{
    io::{self, Write},
    sync::Arc,
};

use crate::{
    postings::PostingEncoder,
    util::{AcqRelU64, RelaxedU32},
    SKIPLIST_BLOCK_LEN,
};

use super::SkipListFormat;

pub trait SkipListWrite {
    fn add_skip_item(&mut self, key: u32, offset: u32, value: Option<u32>) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
}

pub struct SkipListWriter<W: Write> {
    last_key: u32,
    block_len: usize,
    building_block: Arc<BuildingSkipListBlock>,
    item_count_flushed: usize,
    flush_info: Arc<SkipListFlushInfo>,
    writer: W,
    skip_list_format: SkipListFormat,
}

pub struct BuildingSkipListBlock {
    keys: [RelaxedU32; SKIPLIST_BLOCK_LEN],
    offsets: [RelaxedU32; SKIPLIST_BLOCK_LEN],
    values: Option<Box<[RelaxedU32]>>,
}

pub struct SkipListBlockSnapshot {
    len: usize,
    pub keys: Box<[u32]>,
    pub offsets: Box<[u32]>,
    pub values: Option<Box<[u32]>>,
}

pub struct SkipListFlushInfo {
    value: AcqRelU64,
}

pub struct SkipListFlushInfoSnapshot {
    value: u64,
}

impl<W: Write> SkipListWriter<W> {
    pub fn new(skip_list_format: SkipListFormat, writer: W) -> Self {
        let building_block = Arc::new(BuildingSkipListBlock::new(&skip_list_format));
        let flush_info = Arc::new(SkipListFlushInfo::new());

        Self {
            last_key: u32::MAX,
            block_len: 0,
            building_block,
            item_count_flushed: 0,
            flush_info,
            writer,
            skip_list_format,
        }
    }

    pub fn building_block(&self) -> &Arc<BuildingSkipListBlock> {
        &self.building_block
    }

    pub fn flush_info(&self) -> &Arc<SkipListFlushInfo> {
        &self.flush_info
    }

    pub fn skip_list_format(&self) -> &SkipListFormat {
        &self.skip_list_format
    }

    pub fn finish(self) -> W {
        self.writer
    }
}

impl<W: Write> SkipListWrite for SkipListWriter<W> {
    fn add_skip_item(&mut self, key: u32, offset: u32, value: Option<u32>) -> io::Result<()> {
        if self.last_key == u32::MAX {
            self.last_key = 0;
        }
        assert!(key > self.last_key);
        let building_block = self.building_block.as_ref();
        building_block.add_skip_item(key - self.last_key, offset, self.block_len, value);

        self.block_len += 1;
        self.flush_info.set_buffer_len(self.block_len);

        if self.block_len == SKIPLIST_BLOCK_LEN {
            self.flush()?;
        }

        self.last_key = key;

        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.block_len > 0 {
            let building_block = &self.building_block.as_ref();
            let posting_encoder = PostingEncoder;
            let keys = building_block.keys[0..self.block_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            posting_encoder.encode_u32(&keys, &mut self.writer)?;
            let offsets = building_block.offsets[0..self.block_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            posting_encoder.encode_u32(&offsets, &mut self.writer)?;
            if self.skip_list_format.has_value() {
                if let Some(value_atomics) = &building_block.values {
                    let values = value_atomics[0..self.block_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    posting_encoder.encode_u32(&values, &mut self.writer)?;
                }
            }

            self.item_count_flushed += self.block_len;
            self.block_len = 0;
            let mut flush_info = SkipListFlushInfoSnapshot::new(0);
            flush_info.set_buffer_len(self.block_len);
            flush_info.set_flushed_count(self.item_count_flushed);
            self.flush_info.save(flush_info);
        }

        Ok(())
    }
}

pub struct NoSkipListWriter;

impl SkipListWrite for NoSkipListWriter {
    fn add_skip_item(&mut self, _key: u32, _offset: u32, _value: Option<u32>) -> io::Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BuildingSkipListBlock {
    pub fn new(skip_list_format: &SkipListFormat) -> Self {
        let keys = std::iter::repeat_with(|| RelaxedU32::new(0))
            .take(SKIPLIST_BLOCK_LEN)
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap();
        let offsets = std::iter::repeat_with(|| RelaxedU32::new(0))
            .take(SKIPLIST_BLOCK_LEN)
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap();
        let values = if skip_list_format.has_value() {
            Some(
                std::iter::repeat_with(|| RelaxedU32::new(0))
                    .take(SKIPLIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            keys,
            offsets,
            values,
        }
    }

    pub fn snapshot(&self, len: usize) -> SkipListBlockSnapshot {
        let keys = self.keys[0..len].iter().map(|k| k.load()).collect();
        let offsets = self.offsets[0..len]
            .iter()
            .map(|offset| offset.load())
            .collect();
        let values = self
            .values
            .as_ref()
            .map(|values| values[0..len].iter().map(|v| v.load()).collect());

        SkipListBlockSnapshot {
            len,
            keys,
            offsets,
            values,
        }
    }

    fn add_skip_item(&self, key: u32, offset: u32, index: usize, value: Option<u32>) {
        self.keys[index].store(key);
        self.offsets[index].store(offset);
        if let Some(values) = self.values.as_deref() {
            values[index].store(value.unwrap_or_default());
        }
    }
}

impl SkipListBlockSnapshot {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }
}

impl SkipListFlushInfo {
    pub fn new() -> Self {
        Self {
            value: AcqRelU64::new(0),
        }
    }

    pub fn load(&self) -> SkipListFlushInfoSnapshot {
        SkipListFlushInfoSnapshot::new(self.value.load())
    }

    fn save(&self, flush_info: SkipListFlushInfoSnapshot) {
        self.value.store(flush_info.value);
    }

    pub fn flushed_count(&self) -> usize {
        self.load().flushed_count()
    }

    fn set_buffer_len(&self, buffer_len: usize) {
        let mut flush_info = self.load();
        flush_info.set_buffer_len(buffer_len);
        self.save(flush_info);
    }
}

impl SkipListFlushInfoSnapshot {
    const BUFFER_LEN_MASK: u64 = 0xFFFF_FFFF;
    const FLUSHED_COUNT_MASK: u64 = 0xFFFF_FFFF_0000_0000;

    pub fn new(value: u64) -> Self {
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

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            skip_list::{SkipListFormat, SkipListWrite, SkipListWriter},
            PostingEncoder,
        },
        SKIPLIST_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormat::builder().build();
        let mut buf = vec![];
        let mut skip_list_writer = SkipListWriter::new(skip_list_format, &mut buf);
        let flush_info = skip_list_writer.flush_info().clone();

        let keys: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| ((i + 1) * 1000 + i % 8) as u32)
            .collect();
        let keys = &keys[..];
        let keys_encoded: Vec<_> = std::iter::once(keys[0])
            .chain(keys.windows(2).map(|pair| pair[1] - pair[0]))
            .collect();
        let offsets: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| (i * 100 + i % 8) as u32)
            .collect();
        let offsets = &offsets[..];

        for i in 0..BLOCK_LEN * 2 + 3 {
            skip_list_writer.add_skip_item(keys[i], offsets[i], None)?;
        }

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.buffer_len(), 3);
        assert_eq!(flush_info.flushed_count(), BLOCK_LEN * 2);

        skip_list_writer.flush()?;

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.buffer_len(), 0);
        assert_eq!(flush_info.flushed_count(), BLOCK_LEN * 2 + 3);

        let posting_encoder = PostingEncoder;
        let mut decoded_keys = [0; BLOCK_LEN];
        let mut decoded_offsets = [0; BLOCK_LEN];

        let mut reader = BufReader::new(buf.as_slice());

        posting_encoder.decode_u32(&mut reader, &mut decoded_keys)?;
        assert_eq!(&keys_encoded[0..BLOCK_LEN], decoded_keys);
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets)?;
        assert_eq!(&offsets[0..BLOCK_LEN], decoded_offsets);

        posting_encoder.decode_u32(&mut reader, &mut decoded_keys)?;
        assert_eq!(&keys_encoded[BLOCK_LEN..BLOCK_LEN * 2], decoded_keys);
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets)?;
        assert_eq!(&offsets[BLOCK_LEN..BLOCK_LEN * 2], decoded_offsets);

        posting_encoder.decode_u32(&mut reader, &mut decoded_keys[0..3])?;
        assert_eq!(
            &keys_encoded[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_keys[0..3]
        );
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets[0..3])?;
        assert_eq!(
            &offsets[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_offsets[0..3]
        );

        Ok(())
    }
}
