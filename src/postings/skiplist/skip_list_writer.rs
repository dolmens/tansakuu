use std::{io, sync::Arc};

use crate::{
    postings::PostingEncoder,
    util::{AcqRelUsize, RelaxedU32},
    DocId, TermFreq, INVALID_DOCID, SKIPLIST_BLOCK_LEN,
};

use super::{SkipListBlock, SkipListFormat};

pub struct SkipListWriter<W: io::Write> {
    last_docid: DocId,
    block_len: usize,
    building_block: Arc<BuildingSkipListBlock>,
    item_count_flushed: usize,
    flush_info: Arc<SkipListFlushInfo>,
    writer: W,
    skip_list_format: SkipListFormat,
}

pub struct BuildingSkipListBlock {
    len: AcqRelUsize,
    docids: [RelaxedU32; SKIPLIST_BLOCK_LEN],
    offsets: [RelaxedU32; SKIPLIST_BLOCK_LEN],
    termfreqs: Option<Box<[RelaxedU32]>>,
}

pub struct SkipListBlockSnapshot {
    len: usize,
    docids: Box<[DocId]>,
    offsets: Box<[u32]>,
    termfreqs: Option<Box<[TermFreq]>>,
}

pub struct SkipListFlushInfo {
    item_count: AcqRelUsize,
}

impl<W: io::Write> SkipListWriter<W> {
    pub fn new(skip_list_format: SkipListFormat, writer: W) -> Self {
        let building_block = Arc::new(BuildingSkipListBlock::new(&skip_list_format));
        let flush_info = Arc::new(SkipListFlushInfo::new());

        Self {
            last_docid: INVALID_DOCID,
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

    pub fn add_skip_item(
        &mut self,
        last_docid: DocId,
        offset: u32,
        tf: Option<TermFreq>,
    ) -> io::Result<()> {
        if self.last_docid == INVALID_DOCID {
            self.last_docid = 0;
        }
        assert!(last_docid > self.last_docid);
        let building_block = self.building_block.as_ref();
        building_block.add_skip_item(last_docid - self.last_docid, offset, self.block_len, tf);

        self.block_len += 1;
        building_block.len.store(self.block_len);
        if self.block_len == SKIPLIST_BLOCK_LEN {
            self.flush()?;
        }

        self.last_docid = last_docid;

        Ok(())
    }

    fn flush(&mut self) -> io::Result<usize> {
        if self.block_len > 0 {
            let building_block = &self.building_block.as_ref();
            let posting_encoder = PostingEncoder;
            let mut flushed_size = 0;
            let docids = building_block.docids[0..self.block_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            flushed_size += posting_encoder.encode_u32(&docids, &mut self.writer)?;
            let offsets = building_block.offsets[0..self.block_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            flushed_size += posting_encoder.encode_u32(&offsets, &mut self.writer)?;
            if self.skip_list_format.has_tflist() {
                if let Some(termfreqs_atomics) = &building_block.termfreqs {
                    let termfreqs = termfreqs_atomics[0..self.block_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    flushed_size += posting_encoder.encode_u32(&termfreqs, &mut self.writer)?;
                }
            }

            self.item_count_flushed += self.block_len;
            self.flush_info.item_count.store(self.item_count_flushed);

            building_block.clear();
            self.block_len = 0;

            Ok(flushed_size)
        } else {
            Ok(0)
        }
    }
}

impl BuildingSkipListBlock {
    pub fn new(skip_list_format: &SkipListFormat) -> Self {
        let docids = std::iter::repeat_with(|| RelaxedU32::new(0))
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
        let termfreqs = if skip_list_format.has_tflist() {
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
            len: AcqRelUsize::new(0),
            docids,
            offsets,
            termfreqs,
        }
    }

    pub fn snapshot(&self) -> SkipListBlockSnapshot {
        let len = self.len();
        let docids = self.docids[0..len]
            .iter()
            .map(|docid| docid.load())
            .collect();
        let offsets = self.offsets[0..len]
            .iter()
            .map(|offset| offset.load())
            .collect();
        let termfreqs = self
            .termfreqs
            .as_ref()
            .map(|termfreqs| termfreqs[0..len].iter().map(|tf| tf.load()).collect());

        SkipListBlockSnapshot {
            len,
            docids,
            offsets,
            termfreqs,
        }
    }

    pub fn len(&self) -> usize {
        self.len.load()
    }

    fn clear(&self) {
        self.len.store(0);
    }

    fn add_skip_item(&self, last_docid: DocId, offset: u32, index: usize, tf: Option<TermFreq>) {
        self.docids[index].store(last_docid);
        self.offsets[index].store(offset);
        if let Some(termfreqs) = self.termfreqs.as_deref() {
            termfreqs[index].store(tf.unwrap_or_default());
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

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn copy_to(&self, skip_list_block: &mut SkipListBlock) {
        let len = self.len;
        skip_list_block.len = len;
        if len > 0 {
            skip_list_block.docids[0..len].copy_from_slice(&self.docids[0..len]);
            skip_list_block.offsets[0..len].copy_from_slice(&self.offsets[0..len]);
            if let Some(termfreqs) = skip_list_block.termfreqs.as_deref_mut() {
                if let Some(mytermfreqs) = self.termfreqs.as_deref() {
                    termfreqs[0..len].copy_from_slice(&mytermfreqs[0..len]);
                } else {
                    termfreqs[0..len].iter_mut().for_each(|tf| *tf = 0);
                }
            }
        }
    }
}

impl SkipListFlushInfo {
    pub fn new() -> Self {
        Self {
            item_count: AcqRelUsize::new(0),
        }
    }

    pub fn item_count(&self) -> usize {
        self.item_count.load()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            skiplist::{SkipListFormat, SkipListWriter},
            PostingEncoder,
        },
        DocId, SKIPLIST_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = SKIPLIST_BLOCK_LEN;
        let skip_list_format = SkipListFormat::builder().build();
        let mut buf = vec![];
        let mut skip_list_writer = SkipListWriter::new(skip_list_format, &mut buf);
        let building_block = skip_list_writer.building_block().clone();
        let flush_info = skip_list_writer.flush_info().clone();

        let docids: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| ((i + 1) * 1000 + i % 8) as DocId)
            .collect();
        let docids = &docids[..];
        let docids_encoded: Vec<_> = std::iter::once(docids[0])
            .chain(docids.windows(2).map(|pair| pair[1] - pair[0]))
            .collect();
        let offsets: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| (i * 100 + i % 8) as u32)
            .collect();
        let offsets = &offsets[..];

        for i in 0..BLOCK_LEN * 2 + 3 {
            skip_list_writer.add_skip_item(docids[i], offsets[i], None)?;
        }

        assert_eq!(building_block.len(), 3);
        assert_eq!(flush_info.item_count(), BLOCK_LEN * 2);

        skip_list_writer.flush()?;

        assert_eq!(building_block.len(), 0);
        assert_eq!(flush_info.item_count(), BLOCK_LEN * 2 + 3);

        let posting_encoder = PostingEncoder;
        let mut decoded_docids = [0; BLOCK_LEN];
        let mut decoded_offsets = [0; BLOCK_LEN];

        let mut reader = BufReader::new(buf.as_slice());

        posting_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_encoded[0..BLOCK_LEN], decoded_docids);
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets)?;
        assert_eq!(&offsets[0..BLOCK_LEN], decoded_offsets);

        posting_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_encoded[BLOCK_LEN..BLOCK_LEN * 2], decoded_docids);
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets)?;
        assert_eq!(&offsets[BLOCK_LEN..BLOCK_LEN * 2], decoded_offsets);

        posting_encoder.decode_u32(&mut reader, &mut decoded_docids[0..3])?;
        assert_eq!(
            &docids_encoded[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_docids[0..3]
        );
        posting_encoder.decode_u32(&mut reader, &mut decoded_offsets[0..3])?;
        assert_eq!(
            &offsets[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_offsets[0..3]
        );

        Ok(())
    }
}
