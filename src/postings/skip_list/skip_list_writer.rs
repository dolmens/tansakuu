use std::{
    io::{self, Write},
    sync::Arc,
};

use tantivy_common::CountingWriter;

use crate::{
    postings::PostingEncoder,
    util::{AcqRelU64, RelaxedU32},
    SKIPLIST_BLOCK_LEN,
};

use super::SkipListFormat;

pub trait SkipListWrite {
    fn add_skip_item(&mut self, key: u64, offset: u64, value: Option<u64>) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
    fn item_count(&self) -> usize;
    fn written_bytes(&self) -> usize;
}

pub struct SkipListWriter<W: Write> {
    item_count: usize,
    last_key: u64,
    last_offset: u64,
    last_value: u64,
    buffer_len: usize,
    item_count_flushed: usize,
    flush_info: Arc<SkipListFlushInfo>,
    building_block: Arc<BuildingSkipListBlock>,
    output_writer: CountingWriter<W>,
    skip_list_format: SkipListFormat,
}

pub struct BuildingSkipListBlock {
    keys: [RelaxedU32; SKIPLIST_BLOCK_LEN],
    offsets: [RelaxedU32; SKIPLIST_BLOCK_LEN],
    values: Option<Box<[RelaxedU32]>>,
}

#[derive(Default)]
pub struct SkipListBlockSnapshot {
    len: usize,
    pub keys: Option<Box<[u32]>>,
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
    pub fn new(skip_list_format: SkipListFormat, output_writer: W) -> Self {
        let building_block = Arc::new(BuildingSkipListBlock::new(&skip_list_format));
        let flush_info = Arc::new(SkipListFlushInfo::new());

        Self {
            item_count: 0,
            last_key: 0,
            last_offset: 0,
            last_value: 0,
            buffer_len: 0,
            item_count_flushed: 0,
            flush_info,
            building_block,
            output_writer: CountingWriter::wrap(output_writer),
            skip_list_format,
        }
    }

    pub fn flush_info(&self) -> &Arc<SkipListFlushInfo> {
        &self.flush_info
    }

    pub fn building_block(&self) -> &Arc<BuildingSkipListBlock> {
        &self.building_block
    }

    pub fn skip_list_format(&self) -> &SkipListFormat {
        &self.skip_list_format
    }
}

impl<W: Write> SkipListWrite for SkipListWriter<W> {
    fn add_skip_item(&mut self, key: u64, offset: u64, value: Option<u64>) -> io::Result<()> {
        self.item_count += 1;
        let building_block = self.building_block.as_ref();
        building_block.add_skip_item(
            self.buffer_len,
            (key - self.last_key) as u32,
            (offset - self.last_offset) as u32,
            value.map(|value| (value - self.last_value) as u32),
        );

        self.buffer_len += 1;
        let flush_info = SkipListFlushInfoSnapshot::new(self.item_count_flushed, self.buffer_len);
        self.flush_info.store(flush_info);

        if self.buffer_len == SKIPLIST_BLOCK_LEN {
            self.flush()?;
        }

        self.last_key = key;
        self.last_offset = offset;
        self.last_value = value.unwrap_or_default();

        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buffer_len > 0 {
            let building_block = &self.building_block.as_ref();
            let posting_encoder = PostingEncoder;
            let keys = building_block.keys[0..self.buffer_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            posting_encoder.encode_u32(&keys, &mut self.output_writer)?;
            let offsets = building_block.offsets[0..self.buffer_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            posting_encoder.encode_u32(&offsets, &mut self.output_writer)?;
            if self.skip_list_format.has_value() {
                if let Some(value_atomics) = &building_block.values {
                    let values = value_atomics[0..self.buffer_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    posting_encoder.encode_u32(&values, &mut self.output_writer)?;
                }
            }

            self.item_count_flushed += self.buffer_len;
            self.buffer_len = 0;
            let flush_info = SkipListFlushInfoSnapshot::new(self.item_count_flushed, 0);
            self.flush_info.store(flush_info);
        }

        Ok(())
    }

    fn item_count(&self) -> usize {
        self.item_count
    }

    fn written_bytes(&self) -> usize {
        self.output_writer.written_bytes() as usize
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
        if len > 0 {
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
                keys: Some(keys),
                offsets,
                values,
            }
        } else {
            SkipListBlockSnapshot::default()
        }
    }

    fn add_skip_item(&self, index: usize, key: u32, offset: u32, value: Option<u32>) {
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
        SkipListFlushInfoSnapshot::with_value(self.value.load())
    }

    fn store(&self, flush_info: SkipListFlushInfoSnapshot) {
        self.value.store(flush_info.value);
    }
}

impl SkipListFlushInfoSnapshot {
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

#[cfg(test)]
pub use tests::MockSkipListWriter;

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            skip_list::{SkipListFormat, SkipListWrite, SkipListWriter},
            PostingEncoder,
        },
        DocId, SKIPLIST_BLOCK_LEN,
    };

    pub struct MockSkipListWriter<'a> {
        keys: &'a mut Vec<u64>,
        offsets: &'a mut Vec<u64>,
        values: Option<&'a mut Vec<u64>>,
    }

    impl<'a> MockSkipListWriter<'a> {
        pub fn new(
            keys: &'a mut Vec<u64>,
            offsets: &'a mut Vec<u64>,
            values: Option<&'a mut Vec<u64>>,
        ) -> Self {
            Self {
                keys,
                offsets,
                values,
            }
        }
    }

    impl<'a> SkipListWrite for MockSkipListWriter<'a> {
        fn add_skip_item(&mut self, key: u64, offset: u64, value: Option<u64>) -> io::Result<()> {
            self.keys.push(key);
            self.offsets.push(offset);
            self.values
                .as_mut()
                .map(|values| values.push(value.unwrap_or_default()));
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn item_count(&self) -> usize {
            0
        }

        fn written_bytes(&self) -> usize {
            0
        }
    }

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormat::builder().build();
        let mut buf = vec![];
        let mut skip_list_writer = SkipListWriter::new(skip_list_format, &mut buf);
        let flush_info = skip_list_writer.flush_info().clone();

        let docids_deltas: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as DocId).collect();
        let docids_deltas = &docids_deltas[..];
        let docids: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .collect();
        let docids = &docids[..];

        let offsets_deltas: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| (i * 100) as u32)
            .collect();
        let offsets_deltas = &offsets_deltas[..];
        let offsets: Vec<u64> = offsets_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x as u64;
                Some(*acc)
            })
            .collect();
        let offsets = &offsets[..];

        for i in 0..BLOCK_LEN * 2 + 3 {
            skip_list_writer.add_skip_item(docids[i] as u64, offsets[i], None)?;
        }

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.buffer_len(), 3);
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2);

        skip_list_writer.flush()?;

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.buffer_len(), 0);
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2 + 3);

        let posting_encoder = PostingEncoder;
        let mut decoded_keys = [0; BLOCK_LEN];
        let mut decoded_offsets = [0; BLOCK_LEN];

        let mut reader = BufReader::new(buf.as_slice());

        posting_encoder.decode_u32(&mut reader, &mut decoded_keys)?;
        assert_eq!(&docids_deltas[0..BLOCK_LEN], decoded_keys);
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets)?;
        assert_eq!(&offsets_deltas[0..BLOCK_LEN], decoded_offsets);

        posting_encoder.decode_u32(&mut reader, &mut decoded_keys)?;
        assert_eq!(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], decoded_keys);
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets)?;
        assert_eq!(&offsets_deltas[BLOCK_LEN..BLOCK_LEN * 2], decoded_offsets);

        posting_encoder.decode_u32(&mut reader, &mut decoded_keys[0..3])?;
        assert_eq!(
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_keys[0..3]
        );
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets[0..3])?;
        assert_eq!(
            &offsets_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_offsets[0..3]
        );

        Ok(())
    }
}
