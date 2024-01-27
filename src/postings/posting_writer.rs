use std::{
    io::{self, Write},
    sync::Arc,
};

use tantivy_common::CountingWriter;

use crate::{
    util::{AcqRelU64, RelaxedU32, RelaxedU8},
    DocId, POSTING_BLOCK_LEN,
};

use super::{
    positions::{
        none_position_list_writer, EmptyPositionListWriter, PositionListWrite, PositionListWriter,
        PositionListWriterBuilder,
    },
    skip_list::{SkipListWrite, SkipListWriter},
    PostingBlock, PostingEncoder, PostingFormat,
};

pub struct PostingWriter<W: Write, S: SkipListWrite, P: PositionListWrite> {
    doc_count: usize,
    last_docid: DocId,
    current_tf: u32,
    total_tf: u64,
    fieldmask: u8,
    buffer_len: usize,
    doc_count_flushed: usize,
    flush_info: Arc<FlushInfo>,
    building_block: Arc<BuildingPostingBlock>,
    output_writer: CountingWriter<W>,
    skip_list_writer: S,
    position_list_writer: Option<P>,
    posting_format: PostingFormat,
}

pub struct BuildingPostingBlock {
    docids: [RelaxedU32; POSTING_BLOCK_LEN],
    termfreqs: Option<Box<[RelaxedU32]>>,
    fieldmasks: Option<Box<[RelaxedU8]>>,
}

#[derive(Default)]
pub struct PostingBlockSnapshot {
    len: usize,
    docids: Option<Box<[DocId]>>,
    termfreqs: Option<Box<[u32]>>,
    fieldmasks: Option<Box<[u8]>>,
}

pub struct FlushInfo {
    value: AcqRelU64,
}

pub struct FlushInfoSnapshot {
    value: u64,
}

pub struct PostingWriterBuilder<W: Write, S: SkipListWrite, P: PositionListWrite> {
    output_writer: W,
    skip_list_writer: S,
    position_list_writer: Option<P>,
    posting_format: PostingFormat,
}

impl PostingWriterBuilder<io::Sink, SkipListWriter<io::Sink>, EmptyPositionListWriter> {
    pub fn new(posting_format: PostingFormat) -> Self {
        let skip_list_format = posting_format.skip_list_format().clone();
        let skip_list_output = io::sink();
        let skip_list_writer = SkipListWriter::new(skip_list_format, skip_list_output);
        Self {
            output_writer: io::sink(),
            skip_list_writer,
            position_list_writer: none_position_list_writer(),
            posting_format,
        }
    }
}

impl<W: Write, S: SkipListWrite, P: PositionListWrite> PostingWriterBuilder<W, S, P> {
    pub fn with_output_writer<OW: Write>(
        self,
        output_writer: OW,
    ) -> PostingWriterBuilder<OW, S, P> {
        PostingWriterBuilder {
            output_writer,
            skip_list_writer: self.skip_list_writer,
            position_list_writer: self.position_list_writer,
            posting_format: self.posting_format,
        }
    }

    pub fn with_skip_list_output_writer<SW: Write>(
        self,
        skip_list_output_writer: SW,
    ) -> PostingWriterBuilder<W, SkipListWriter<SW>, P> {
        let skip_list_format = self.posting_format.skip_list_format().clone();
        let skip_list_writer = SkipListWriter::new(skip_list_format, skip_list_output_writer);
        PostingWriterBuilder {
            output_writer: self.output_writer,
            skip_list_writer,
            position_list_writer: self.position_list_writer,
            posting_format: self.posting_format,
        }
    }

    pub fn with_skip_list_writer<SW: SkipListWrite>(
        self,
        skip_list_writer: SW,
    ) -> PostingWriterBuilder<W, SW, P> {
        PostingWriterBuilder {
            output_writer: self.output_writer,
            skip_list_writer,
            position_list_writer: self.position_list_writer,
            posting_format: self.posting_format,
        }
    }

    pub fn with_position_list_output_writer<PW: Write, PSW: Write>(
        self,
        writer: PW,
        skip_list_writer: PSW,
    ) -> PostingWriterBuilder<W, S, PositionListWriter<PW, SkipListWriter<PSW>>> {
        let position_list_writer = Some(
            PositionListWriterBuilder::new(writer)
                .with_skip_list_output_writer(skip_list_writer)
                .build(),
        );

        PostingWriterBuilder {
            output_writer: self.output_writer,
            skip_list_writer: self.skip_list_writer,
            position_list_writer,
            posting_format: self.posting_format,
        }
    }

    pub fn with_position_list_writer<PW: PositionListWrite>(
        self,
        position_list_writer: Option<PW>,
    ) -> PostingWriterBuilder<W, S, PW> {
        PostingWriterBuilder {
            output_writer: self.output_writer,
            skip_list_writer: self.skip_list_writer,
            position_list_writer,
            posting_format: self.posting_format,
        }
    }

    pub fn build(self) -> PostingWriter<W, S, P> {
        PostingWriter::new(
            self.posting_format,
            self.output_writer,
            self.skip_list_writer,
            self.position_list_writer,
        )
    }
}

impl<W: Write, S: SkipListWrite, P: PositionListWrite> PostingWriter<W, S, P> {
    pub fn new(
        posting_format: PostingFormat,
        output_writer: W,
        skip_list_writer: S,
        position_list_writer: Option<P>,
    ) -> Self {
        let building_block = Arc::new(BuildingPostingBlock::new(&posting_format));
        let flush_info = Arc::new(FlushInfo::new());
        Self {
            doc_count: 0,
            last_docid: 0,
            current_tf: 0,
            total_tf: 0,
            fieldmask: 0,
            buffer_len: 0,
            doc_count_flushed: 0,
            flush_info,
            building_block,
            output_writer: CountingWriter::wrap(output_writer),
            skip_list_writer,
            position_list_writer,
            posting_format,
        }
    }

    pub fn flush_info(&self) -> &Arc<FlushInfo> {
        &self.flush_info
    }

    pub fn building_block(&self) -> &Arc<BuildingPostingBlock> {
        &self.building_block
    }

    pub fn posting_format(&self) -> &PostingFormat {
        &self.posting_format
    }

    pub fn add_pos(&mut self, _field: usize, pos: u32) -> io::Result<()> {
        self.current_tf += 1;
        self.total_tf += 1;

        if let Some(position_list_writer) = self.position_list_writer.as_mut() {
            position_list_writer.add_pos(pos)?;
        }

        Ok(())
    }

    pub fn end_doc(&mut self, docid: DocId) -> io::Result<()> {
        assert!(self.doc_count == 0 || docid > self.last_docid);
        self.doc_count += 1;
        if let Some(position_list_writer) = self.position_list_writer.as_mut() {
            position_list_writer.end_doc();
        }
        let building_block = self.building_block.as_ref();
        building_block.add_docid(self.buffer_len, docid - self.last_docid);
        building_block.add_tf(self.buffer_len, self.current_tf);
        building_block.add_fieldmask(self.buffer_len, self.fieldmask);

        self.last_docid = docid;
        self.current_tf = 0;

        self.buffer_len += 1;
        let flush_info = FlushInfoSnapshot::new(self.doc_count_flushed, self.buffer_len);
        self.flush_info.store(flush_info);

        if self.buffer_len == POSTING_BLOCK_LEN {
            self.flush_buffer()?;
        }

        Ok(())
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer_len > 0 {
            let building_block = self.building_block.as_ref();
            let posting_encoder = PostingEncoder;
            let docids = building_block.docids[0..self.buffer_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            posting_encoder.encode_u32(&docids, &mut self.output_writer)?;
            if self.posting_format.has_tflist() {
                if let Some(termfreqs_atomics) = &building_block.termfreqs {
                    let termfreqs = termfreqs_atomics[0..self.buffer_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    posting_encoder.encode_u32(&termfreqs, &mut self.output_writer)?;
                }
            }
            if self.posting_format.has_fieldmask() {
                if let Some(fieldmaps_atomics) = &building_block.fieldmasks {
                    let fieldmaps = fieldmaps_atomics[0..self.buffer_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    posting_encoder.encode_u8(&fieldmaps, &mut self.output_writer)?;
                }
            }

            self.skip_list_writer.add_skip_item(
                self.last_docid as u64,
                self.output_writer.written_bytes(),
                Some(self.total_tf),
            )?;

            self.doc_count_flushed += self.buffer_len;
            self.buffer_len = 0;
            let flush_info = FlushInfoSnapshot::new(self.doc_count_flushed, 0);
            self.flush_info.store(flush_info);
        }

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        if let Some(position_list_writer) = self.position_list_writer.as_mut() {
            position_list_writer.flush()?;
        }
        self.skip_list_writer.flush()?;

        Ok(())
    }

    pub fn item_count(&self) -> (usize, usize) {
        (self.doc_count, self.skip_list_writer.item_count())
    }

    pub fn position_list_item_count(&self) -> Option<(usize, usize)> {
        self.position_list_writer.as_ref().map(|p| p.item_count())
    }

    pub fn written_bytes(&self) -> (usize, usize) {
        (
            self.output_writer.written_bytes() as usize,
            self.skip_list_writer.written_bytes(),
        )
    }

    pub fn position_list_written_bytes(&self) -> Option<(usize, usize)> {
        self.position_list_writer
            .as_ref()
            .map(|p| p.written_bytes())
    }
}

impl BuildingPostingBlock {
    pub fn new(posting_format: &PostingFormat) -> Self {
        const ZERO: RelaxedU32 = RelaxedU32::new(0);
        let docids = [ZERO; POSTING_BLOCK_LEN];
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
        if len > 0 {
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
                docids: Some(docids),
                termfreqs,
                fieldmasks,
            }
        } else {
            PostingBlockSnapshot::default()
        }
    }

    fn add_docid(&self, offset: usize, docid: DocId) {
        self.docids[offset].store(docid);
    }

    fn add_tf(&self, offset: usize, tf: u32) {
        self.termfreqs.as_ref().map(|termfreqs| {
            termfreqs[offset].store(tf);
        });
    }

    fn add_fieldmask(&self, offset: usize, fieldmask: u8) {
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
            posting_block.docids[0..len].copy_from_slice(&self.docids.as_ref().unwrap()[0..len]);
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
        FlushInfoSnapshot::with_value(self.value.load())
    }

    fn store(&self, flush_info: FlushInfoSnapshot) {
        self.value.store(flush_info.value);
    }
}

impl FlushInfoSnapshot {
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
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            positions::none_position_list_writer, skip_list::SkipListWriter, PostingEncoder,
            PostingFormat,
        },
        DocId, POSTING_BLOCK_LEN,
    };

    use super::PostingWriter;

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = POSTING_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let skip_list_format = posting_format.skip_list_format().clone();
        let mut buf = vec![];
        let mut skip_list_buf = vec![];
        let skip_list_writer = SkipListWriter::new(skip_list_format, &mut skip_list_buf);
        let mut posting_writer = PostingWriter::new(
            posting_format,
            &mut buf,
            skip_list_writer,
            none_position_list_writer(),
        );
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
            .map(|(i, _)| (i % 3 + 1) as u32)
            .collect();
        let termfreqs = &termfreqs[..];

        for _ in 0..termfreqs[0] {
            posting_writer.add_pos(0, 1)?;
        }
        posting_writer.end_doc(docids[0])?;

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), 0);
        assert_eq!(flush_info_snapshot.buffer_len(), 1);
        assert_eq!(building_block.docids[0].load(), docids[0]);
        assert_eq!(
            building_block.termfreqs.as_ref().unwrap()[0].load(),
            termfreqs[0]
        );

        for _ in 0..termfreqs[1] {
            posting_writer.add_pos(0, 1)?;
        }
        posting_writer.end_doc(docids[1])?;

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
                posting_writer.add_pos(0, 1)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        let flush_info_snapshot = flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(0, 1)?;
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN])?;
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