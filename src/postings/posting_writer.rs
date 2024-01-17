use std::{
    io::{self, Write},
    sync::Arc,
};

use crate::{
    util::{AcqRelU64, RelaxedU32, RelaxedU8},
    DocId, FieldMask, TermFreq, INVALID_DOCID, POSTING_BLOCK_LEN,
};

use super::{
    skip_list::{NoSkipListWriter, SkipListWrite},
    PostingBlock, PostingEncoder, PostingFormat,
};

pub struct PostingWriter<W: Write, S: SkipListWrite = NoSkipListWriter> {
    last_docid: DocId,
    current_tf: TermFreq,
    total_tf: TermFreq,
    fieldmask: FieldMask,
    block_len: usize,
    building_block: Arc<BuildingPostingBlock>,
    doc_count_flushed: usize,
    flush_info: Arc<FlushInfo>,
    writer: W,
    skip_list_writer: S,
    posting_format: PostingFormat,
}

pub struct BuildingPostingBlock {
    docids: [RelaxedU32; POSTING_BLOCK_LEN],
    termfreqs: Option<Box<[RelaxedU32]>>,
    fieldmasks: Option<Box<[RelaxedU8]>>,
}

pub struct PostingBlockSnapshot {
    len: usize,
    docids: Box<[DocId]>,
    termfreqs: Option<Box<[TermFreq]>>,
    fieldmasks: Option<Box<[FieldMask]>>,
}

pub struct FlushInfo {
    value: AcqRelU64,
}

pub struct FlushInfoSnapshot {
    value: u64,
}

impl<W: Write> PostingWriter<W, NoSkipListWriter> {
    pub fn new(posting_format: PostingFormat, writer: W) -> Self {
        Self::new_with_skip_list(posting_format, writer, NoSkipListWriter)
    }
}

impl<W: Write, S: SkipListWrite> PostingWriter<W, S> {
    pub fn new_with_skip_list(
        posting_format: PostingFormat,
        writer: W,
        skip_list_writer: S,
    ) -> Self {
        let building_block = Arc::new(BuildingPostingBlock::new(&posting_format));
        let flush_info = Arc::new(FlushInfo::new());

        Self {
            last_docid: INVALID_DOCID,
            current_tf: 0,
            total_tf: 0,
            fieldmask: 0,
            block_len: 0,
            building_block,
            doc_count_flushed: 0,
            flush_info,
            writer,
            skip_list_writer,
            posting_format,
        }
    }

    pub fn building_block(&self) -> &Arc<BuildingPostingBlock> {
        &self.building_block
    }

    pub fn flush_info(&self) -> &Arc<FlushInfo> {
        &self.flush_info
    }

    pub fn posting_format(&self) -> &PostingFormat {
        &self.posting_format
    }

    pub fn add_pos(&mut self, _field: usize) {
        self.current_tf += 1;
        self.total_tf += 1;
    }

    pub fn end_doc(&mut self, docid: DocId) {
        if self.last_docid == INVALID_DOCID {
            self.last_docid = 0;
        } else {
            assert!(docid > self.last_docid);
        }
        let building_block = self.building_block.as_ref();
        building_block.add_docid(self.block_len, docid - self.last_docid);
        building_block.add_tf(self.block_len, self.current_tf);
        building_block.add_fieldmask(self.block_len, self.fieldmask);

        self.last_docid = docid;
        self.current_tf = 0;

        self.block_len += 1;
        self.flush_info.set_buffer_len(self.block_len);

        if self.block_len == POSTING_BLOCK_LEN {
            self.flush().unwrap();
        }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.block_len > 0 {
            let building_block = self.building_block.as_ref();
            let posting_encoder = PostingEncoder;
            let mut flushed_size = 0;
            let docids = building_block.docids[0..self.block_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            flushed_size += posting_encoder.encode_u32(&docids, &mut self.writer)?;
            if self.posting_format.has_tflist() {
                if let Some(termfreqs_atomics) = &building_block.termfreqs {
                    let termfreqs = termfreqs_atomics[0..self.block_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    flushed_size += posting_encoder.encode_u32(&termfreqs, &mut self.writer)?;
                }
            }
            if self.posting_format.has_fieldmask() {
                if let Some(fieldmaps_atomics) = &building_block.fieldmasks {
                    let fieldmaps = fieldmaps_atomics[0..self.block_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    flushed_size += posting_encoder.encode_u8(&fieldmaps, &mut self.writer)?;
                }
            }

            self.doc_count_flushed += self.block_len;
            self.block_len = 0;
            let mut flush_info = FlushInfoSnapshot::new(0);
            flush_info.set_buffer_len(self.block_len);
            flush_info.set_flushed_count(self.doc_count_flushed);
            self.flush_info.save(flush_info);

            // Only full block will have a skip item
            if self.block_len == POSTING_BLOCK_LEN {
                self.skip_list_writer
                    .add_skip_item(self.last_docid, flushed_size as u32, None)?;
            }
        }

        Ok(())
    }

    pub fn finish(self) -> (W, S) {
        (self.writer, self.skip_list_writer)
    }
}

impl BuildingPostingBlock {
    pub fn new(posting_format: &PostingFormat) -> Self {
        let docids = std::iter::repeat_with(|| RelaxedU32::new(0))
            .take(POSTING_BLOCK_LEN)
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap();
        let termfreqs = if posting_format.has_tflist() {
            Some(
                std::iter::repeat_with(|| RelaxedU32::new(0))
                    .take(POSTING_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };
        let fieldmasks = if posting_format.has_fieldmask() {
            Some(
                std::iter::repeat_with(|| RelaxedU8::new(0))
                    .take(POSTING_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            docids,
            termfreqs,
            fieldmasks,
        }
    }

    pub fn snapshot(&self, len: usize) -> PostingBlockSnapshot {
        let docids = self.docids[0..len]
            .iter()
            .map(|docid| docid.load())
            .collect();
        let termfreqs = self
            .termfreqs
            .as_ref()
            .map(|termfreqs| termfreqs[0..len].iter().map(|tf| tf.load()).collect());
        let fieldmasks = self
            .fieldmasks
            .as_ref()
            .map(|fieldmasks| fieldmasks[0..len].iter().map(|fm| fm.load()).collect());

        PostingBlockSnapshot {
            len,
            docids,
            termfreqs,
            fieldmasks,
        }
    }

    fn add_docid(&self, offset: usize, docid: DocId) {
        self.docids[offset].store(docid);
    }

    fn add_tf(&self, offset: usize, tf: TermFreq) {
        self.termfreqs.as_ref().map(|termfreqs| {
            termfreqs[offset].store(tf);
        });
    }

    fn add_fieldmask(&self, offset: usize, fieldmask: FieldMask) {
        self.fieldmasks
            .as_ref()
            .map(|fieldmasks| fieldmasks[offset].store(fieldmask));
    }
}

impl PostingBlockSnapshot {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn copy_to(&self, posting_block: &mut PostingBlock) {
        let len = self.len;
        posting_block.len = len;
        if len > 0 {
            posting_block.docids[0..len].copy_from_slice(&self.docids[0..len]);
            if let Some(termfreqs) = &mut posting_block.termfreqs {
                if let Some(mytermfreqs) = &self.termfreqs {
                    termfreqs[0..len].copy_from_slice(&mytermfreqs[0..len]);
                } else {
                    termfreqs[0..len].iter_mut().for_each(|tf| *tf = 0);
                }
            }
            if let Some(fieldmasks) = &mut posting_block.fieldmasks {
                if let Some(myfieldmasks) = &self.fieldmasks {
                    fieldmasks[0..len].copy_from_slice(&myfieldmasks[0..len]);
                } else {
                    fieldmasks[0..len].iter_mut().for_each(|fm| *fm = 0);
                }
            }
        }
    }
}

impl FlushInfo {
    pub fn new() -> Self {
        Self {
            value: AcqRelU64::new(0),
        }
    }

    pub fn load(&self) -> FlushInfoSnapshot {
        FlushInfoSnapshot::new(self.value.load())
    }

    fn save(&self, flush_info: FlushInfoSnapshot) {
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

impl FlushInfoSnapshot {
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
        postings::{PostingEncoder, PostingFormat},
        DocId, TermFreq, POSTING_BLOCK_LEN,
    };

    use super::PostingWriter;

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let mut buf = vec![];
        let mut posting_writer = PostingWriter::new(posting_format, &mut buf);
        let building_block = posting_writer.building_block().clone();
        let flush_info = posting_writer.flush_info().clone();

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

        let termfreqs: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| (i % 3 + 1) as TermFreq)
            .collect();
        let termfreqs = &termfreqs[..];

        for _ in 0..termfreqs[0] {
            posting_writer.add_pos(1);
        }
        posting_writer.end_doc(docids[0]);

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), 0);
        assert_eq!(flush_info_snapshot.buffer_len(), 1);
        assert_eq!(building_block.docids[0].load(), docids[0]);
        assert_eq!(
            building_block.termfreqs.as_ref().unwrap()[0].load(),
            termfreqs[0]
        );

        for _ in 0..termfreqs[1] {
            posting_writer.add_pos(1);
        }
        posting_writer.end_doc(docids[1]);

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), 0);
        assert_eq!(flush_info_snapshot.buffer_len(), 2);
        assert_eq!(building_block.docids[0].load(), docids[0]);
        assert_eq!(
            building_block.termfreqs.as_ref().unwrap()[0].load(),
            termfreqs[0]
        );
        assert_eq!(building_block.docids[1].load(), docids[1]);
        assert_eq!(
            building_block.termfreqs.as_ref().unwrap()[1].load(),
            termfreqs[1]
        );

        for i in 2..BLOCK_LEN {
            for _ in 0..termfreqs[i] {
                posting_writer.add_pos(1);
            }
            posting_writer.end_doc(docids[i]);
        }

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(1);
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN]);
        }

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2);
        assert_eq!(flush_info_snapshot.buffer_len(), 3);

        posting_writer.flush()?;

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2 + 3);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        let posting_encoder = PostingEncoder;

        let mut decoded_docids = [0; BLOCK_LEN];
        let mut decoded_termfreqs = [0; BLOCK_LEN];

        let mut reader = BufReader::new(buf.as_slice());
        posting_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_deltas[0..BLOCK_LEN], decoded_docids);
        posting_encoder.decode_u32(&mut reader, &mut decoded_termfreqs)?;
        assert_eq!(&termfreqs[0..BLOCK_LEN], decoded_termfreqs);

        posting_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], decoded_docids);
        posting_encoder.decode_u32(&mut reader, &mut decoded_termfreqs)?;
        assert_eq!(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], decoded_termfreqs);

        posting_encoder.decode_u32(&mut reader, &mut decoded_docids[0..3])?;
        assert_eq!(
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_docids[0..3]
        );
        posting_encoder.decode_u32(&mut reader, &mut decoded_termfreqs[0..3])?;
        assert_eq!(
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_termfreqs[0..3]
        );

        Ok(())
    }
}
