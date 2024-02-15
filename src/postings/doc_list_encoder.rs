use std::{
    io::{self, Write},
    sync::Arc,
};

use tantivy_common::CountingWriter;

use crate::{
    util::atomic::{AcqRelU64, RelaxedU32, RelaxedU8},
    DocId, DOC_LIST_BLOCK_LEN, MAX_UNCOMPRESSED_DOC_LIST_LEN,
};

use super::{
    compression::BlockEncoder,
    skip_list::{BasicSkipListWriter, SkipListWrite, SkipListWriter},
    DocListBlock, DocListFormat,
};

pub trait DocListEncode {
    fn add_pos(&mut self, field: usize) -> io::Result<()>;
    fn end_doc(&mut self, docid: DocId) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
    fn df(&self) -> usize;
    fn written_bytes(&self) -> (usize, usize);
}

pub struct DocListEncoder<W: Write, S: SkipListWrite> {
    df: usize,
    last_docid: DocId,
    current_tf: u32,
    total_tf: u64,
    fieldmask: u8,
    buffer_len: usize,
    doc_count_flushed: usize,
    building_block: Arc<BuildingDocListBlock>,
    writer: CountingWriter<W>,
    skip_list_writer: S,
    doc_list_format: DocListFormat,
}

pub struct DocListEncoderBuilder<W: Write, S: SkipListWrite> {
    writer: W,
    skip_list_writer: S,
    doc_list_format: DocListFormat,
}

pub struct DocListFlushInfo {
    value: AcqRelU64,
}

pub struct DocListFlushInfoSnapshot {
    value: u64,
}

pub struct BuildingDocListBlock {
    pub flush_info: DocListFlushInfo,
    pub docids: [RelaxedU32; DOC_LIST_BLOCK_LEN],
    pub termfreqs: Option<Box<[RelaxedU32]>>,
    pub fieldmasks: Option<Box<[RelaxedU8]>>,
}

#[derive(Default)]
pub struct DocListBlockSnapshot {
    len: usize,
    docids: Option<Box<[DocId]>>,
    termfreqs: Option<Box<[u32]>>,
    fieldmasks: Option<Box<[u8]>>,
}

pub fn doc_list_encoder_builder(
    doc_list_format: DocListFormat,
) -> DocListEncoderBuilder<io::Sink, BasicSkipListWriter> {
    DocListEncoderBuilder {
        writer: io::sink(),
        skip_list_writer: BasicSkipListWriter::default(),
        doc_list_format,
    }
}

impl<W: Write, S: SkipListWrite> DocListEncoderBuilder<W, S> {
    pub fn with_writer<OW: Write>(self, writer: OW) -> DocListEncoderBuilder<OW, S> {
        DocListEncoderBuilder {
            writer,
            skip_list_writer: self.skip_list_writer,
            doc_list_format: self.doc_list_format,
        }
    }

    pub fn with_skip_list_output_writer<SW: Write>(
        self,
        skip_list_output_writer: SW,
    ) -> DocListEncoderBuilder<W, SkipListWriter<SW>> {
        let skip_list_foramt = self.doc_list_format.skip_list_format().clone();
        let skip_list_writer = SkipListWriter::new(skip_list_foramt, skip_list_output_writer);
        DocListEncoderBuilder {
            writer: self.writer,
            skip_list_writer,
            doc_list_format: self.doc_list_format,
        }
    }

    pub fn with_skip_list_writer<SW: SkipListWrite>(
        self,
        skip_list_writer: SW,
    ) -> DocListEncoderBuilder<W, SW> {
        DocListEncoderBuilder {
            writer: self.writer,
            skip_list_writer,
            doc_list_format: self.doc_list_format,
        }
    }

    pub fn build(self) -> DocListEncoder<W, S> {
        DocListEncoder::new(self.doc_list_format, self.writer, self.skip_list_writer)
    }
}

impl<W: Write, S: SkipListWrite> DocListEncoder<W, S> {
    pub fn new(doc_list_format: DocListFormat, writer: W, skip_list_writer: S) -> Self {
        let building_block = Arc::new(BuildingDocListBlock::new(&doc_list_format));
        Self {
            df: 0,
            last_docid: 0,
            current_tf: 0,
            total_tf: 0,
            fieldmask: 0,
            buffer_len: 0,
            doc_count_flushed: 0,
            building_block,
            writer: CountingWriter::wrap(writer),
            skip_list_writer,
            doc_list_format,
        }
    }

    pub fn building_block(&self) -> &Arc<BuildingDocListBlock> {
        &self.building_block
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer_len > 0 {
            let building_block = self.building_block.as_ref();
            let block_encoder = BlockEncoder;
            let docids = building_block.docids[0..self.buffer_len]
                .iter()
                .map(|a| a.load())
                .collect::<Vec<_>>();
            block_encoder.encode_u32(&docids, &mut self.writer)?;
            if self.doc_list_format.has_tflist() {
                if let Some(termfreqs_atomics) = &building_block.termfreqs {
                    let termfreqs = termfreqs_atomics[0..self.buffer_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    block_encoder.encode_u32(&termfreqs, &mut self.writer)?;
                }
            }
            if self.doc_list_format.has_fieldmask() {
                if let Some(fieldmaps_atomics) = &building_block.fieldmasks {
                    let fieldmaps = fieldmaps_atomics[0..self.buffer_len]
                        .iter()
                        .map(|a| a.load())
                        .collect::<Vec<_>>();
                    block_encoder.encode_u8(&fieldmaps, &mut self.writer)?;
                }
            }

            if self.df > MAX_UNCOMPRESSED_DOC_LIST_LEN {
                self.skip_list_writer.add_skip_item(
                    self.last_docid as u64,
                    self.writer.written_bytes(),
                    Some(self.total_tf),
                )?;
            }

            self.doc_count_flushed += self.buffer_len;
            self.buffer_len = 0;
            let flush_info = DocListFlushInfoSnapshot::new(self.doc_count_flushed, 0);
            self.building_block.flush_info.store(flush_info);
        }

        Ok(())
    }
}

impl<W: Write, S: SkipListWrite> DocListEncode for DocListEncoder<W, S> {
    fn add_pos(&mut self, field: usize) -> io::Result<()> {
        self.current_tf += 1;
        self.total_tf += 1;
        debug_assert!(field < 8);
        self.fieldmask |= 1 << field;

        Ok(())
    }

    fn end_doc(&mut self, docid: DocId) -> io::Result<()> {
        assert!(self.df == 0 || docid > self.last_docid);
        self.df += 1;

        let building_block = self.building_block.as_ref();
        building_block.add_docid(self.buffer_len, docid - self.last_docid);
        building_block.add_tf(self.buffer_len, self.current_tf);
        building_block.add_fieldmask(self.buffer_len, self.fieldmask);

        self.last_docid = docid;
        self.current_tf = 0;
        self.fieldmask = 0;

        self.buffer_len += 1;
        let flush_info = DocListFlushInfoSnapshot::new(self.doc_count_flushed, self.buffer_len);
        self.building_block.flush_info.store(flush_info);

        if self.buffer_len == DOC_LIST_BLOCK_LEN {
            self.flush_buffer()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.skip_list_writer.flush()?;

        Ok(())
    }

    fn df(&self) -> usize {
        self.df
    }

    fn written_bytes(&self) -> (usize, usize) {
        (
            self.writer.written_bytes() as usize,
            self.skip_list_writer.written_bytes(),
        )
    }
}

impl DocListFlushInfo {
    pub fn new() -> Self {
        Self {
            value: AcqRelU64::new(0),
        }
    }

    pub fn load(&self) -> DocListFlushInfoSnapshot {
        DocListFlushInfoSnapshot::with_value(self.value.load())
    }

    fn store(&self, flush_info: DocListFlushInfoSnapshot) {
        self.value.store(flush_info.value);
    }
}

impl DocListFlushInfoSnapshot {
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

impl BuildingDocListBlock {
    pub fn new(doc_list_format: &DocListFormat) -> Self {
        let flush_info = DocListFlushInfo::new();
        const ZERO: RelaxedU32 = RelaxedU32::new(0);
        let docids = [ZERO; DOC_LIST_BLOCK_LEN];
        let termfreqs = if doc_list_format.has_tflist() {
            Some(
                std::iter::repeat_with(|| RelaxedU32::new(0))
                    .take(DOC_LIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };
        let fieldmasks = if doc_list_format.has_fieldmask() {
            Some(
                std::iter::repeat_with(|| RelaxedU8::new(0))
                    .take(DOC_LIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            flush_info,
            docids,
            termfreqs,
            fieldmasks,
        }
    }

    pub fn snapshot(&self, len: usize) -> DocListBlockSnapshot {
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

            DocListBlockSnapshot {
                len,
                docids: Some(docids),
                termfreqs,
                fieldmasks,
            }
        } else {
            DocListBlockSnapshot::default()
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

impl DocListBlockSnapshot {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn docids(&self) -> Option<&[DocId]> {
        self.docids.as_deref().map(|docids| &docids[0..self.len])
    }

    pub fn termfreqs(&self) -> Option<&[u32]> {
        self.termfreqs
            .as_deref()
            .map(|termfreqs| &termfreqs[0..self.len])
    }

    pub fn fieldmasks(&self) -> Option<&[u8]> {
        self.fieldmasks
            .as_deref()
            .map(|fieldmasks| &fieldmasks[0..self.len])
    }

    pub fn copy_to(&self, doc_list_block: &mut DocListBlock) {
        let len = self.len;
        doc_list_block.len = len;
        if len > 0 {
            doc_list_block.docids[0..len].copy_from_slice(&self.docids.as_ref().unwrap()[0..len]);
            if let Some(termfreqs) = &mut doc_list_block.termfreqs {
                if let Some(mytermfreqs) = &self.termfreqs {
                    termfreqs[0..len].copy_from_slice(&mytermfreqs[0..len]);
                } else {
                    termfreqs[0..len].iter_mut().for_each(|tf| *tf = 0);
                }
            }
            if let Some(fieldmasks) = &mut doc_list_block.fieldmasks {
                if let Some(myfieldmasks) = &self.fieldmasks {
                    fieldmasks[0..len].copy_from_slice(&myfieldmasks[0..len]);
                } else {
                    fieldmasks[0..len].iter_mut().for_each(|fm| *fm = 0);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use crate::{
        postings::{
            compression::BlockEncoder, skip_list::SkipListWriter, DocListEncode, DocListEncoder,
            DocListFormat,
        },
        DocId, DOC_LIST_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let doc_list_format = DocListFormat::builder().with_tflist().build();
        let skip_list_format = doc_list_format.skip_list_format().clone();
        let mut buf = vec![];
        let mut skip_list_buf = vec![];
        let skip_list_writer = SkipListWriter::new(skip_list_format, &mut skip_list_buf);
        let mut doc_list_encoder = DocListEncoder::new(doc_list_format, &mut buf, skip_list_writer);
        let building_block = doc_list_encoder.building_block().clone();

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
            doc_list_encoder.add_pos(0)?;
        }
        doc_list_encoder.end_doc(docids[0])?;

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), 0);
        assert_eq!(flush_info_snapshot.buffer_len(), 1);
        assert_eq!(building_block.docids[0].load(), docids[0]);
        assert_eq!(
            building_block.termfreqs.as_ref().unwrap()[0].load(),
            termfreqs[0]
        );

        for _ in 0..termfreqs[1] {
            doc_list_encoder.add_pos(0)?;
        }
        doc_list_encoder.end_doc(docids[1])?;

        let flush_info_snapshot = building_block.flush_info.load();
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
                doc_list_encoder.add_pos(0)?;
            }
            doc_list_encoder.end_doc(docids[i])?;
        }

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                doc_list_encoder.add_pos(0)?;
            }
            doc_list_encoder.end_doc(docids[i + BLOCK_LEN])?;
        }

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2);
        assert_eq!(flush_info_snapshot.buffer_len(), 3);

        doc_list_encoder.flush()?;

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2 + 3);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        let block_encoder = BlockEncoder;

        let mut decoded_docids = [0; BLOCK_LEN];
        let mut decoded_termfreqs = [0; BLOCK_LEN];

        let mut reader = BufReader::new(buf.as_slice());
        block_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_deltas[0..BLOCK_LEN], decoded_docids);
        block_encoder.decode_u32(&mut reader, &mut decoded_termfreqs)?;
        assert_eq!(&termfreqs[0..BLOCK_LEN], decoded_termfreqs);

        block_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], decoded_docids);
        block_encoder.decode_u32(&mut reader, &mut decoded_termfreqs)?;
        assert_eq!(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], decoded_termfreqs);

        block_encoder.decode_u32(&mut reader, &mut decoded_docids[0..3])?;
        assert_eq!(
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_docids[0..3]
        );
        block_encoder.decode_u32(&mut reader, &mut decoded_termfreqs[0..3])?;
        assert_eq!(
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_termfreqs[0..3]
        );

        Ok(())
    }

    #[test]
    fn test_with_fieldmask() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let doc_list_format = DocListFormat::builder().with_fieldmask().build();
        let skip_list_format = doc_list_format.skip_list_format().clone();
        let mut buf = vec![];
        let mut skip_list_buf = vec![];
        let skip_list_writer = SkipListWriter::new(skip_list_format, &mut skip_list_buf);
        let mut doc_list_encoder = DocListEncoder::new(doc_list_format, &mut buf, skip_list_writer);
        let building_block = doc_list_encoder.building_block().clone();

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

        let mut fields = vec![];
        for &tf in termfreqs {
            let mut one_fields = vec![];
            for t in 0..tf {
                one_fields.push((t % 8) as usize);
            }
            fields.push(one_fields);
        }
        let fieldmasks: Vec<_> = fields
            .iter()
            .map(|fields| fields.iter().fold(0 as u8, |acc, &f| acc | (1 << f)))
            .collect();

        for t in 0..termfreqs[0] {
            doc_list_encoder.add_pos(fields[0][t as usize])?;
        }
        doc_list_encoder.end_doc(docids[0])?;

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), 0);
        assert_eq!(flush_info_snapshot.buffer_len(), 1);
        assert_eq!(building_block.docids[0].load(), docids[0]);
        assert_eq!(
            building_block.fieldmasks.as_ref().unwrap()[0].load(),
            fieldmasks[0]
        );

        for t in 0..termfreqs[1] {
            doc_list_encoder.add_pos(fields[1][t as usize])?;
        }
        doc_list_encoder.end_doc(docids[1])?;

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), 0);
        assert_eq!(flush_info_snapshot.buffer_len(), 2);
        assert_eq!(building_block.docids[0].load(), docids[0]);
        assert_eq!(
            building_block.fieldmasks.as_ref().unwrap()[0].load(),
            fieldmasks[0]
        );
        assert_eq!(building_block.docids[1].load(), docids[1]);
        assert_eq!(
            building_block.fieldmasks.as_ref().unwrap()[1].load(),
            fieldmasks[1]
        );

        for i in 2..BLOCK_LEN {
            for t in 0..termfreqs[i] {
                doc_list_encoder.add_pos(fields[i][t as usize])?;
            }
            doc_list_encoder.end_doc(docids[i])?;
        }

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        for i in 0..BLOCK_LEN + 3 {
            for t in 0..termfreqs[i + BLOCK_LEN] {
                doc_list_encoder.add_pos(fields[i + BLOCK_LEN][t as usize])?;
            }
            doc_list_encoder.end_doc(docids[i + BLOCK_LEN])?;
        }

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2);
        assert_eq!(flush_info_snapshot.buffer_len(), 3);

        doc_list_encoder.flush()?;

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2 + 3);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        let block_encoder = BlockEncoder;

        let mut decoded_docids = [0; BLOCK_LEN];
        let mut decoded_fieldmasks = [0; BLOCK_LEN];

        let mut reader = BufReader::new(buf.as_slice());
        block_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_deltas[0..BLOCK_LEN], decoded_docids);
        block_encoder.decode_u8(&mut reader, &mut decoded_fieldmasks)?;
        assert_eq!(&fieldmasks[0..BLOCK_LEN], decoded_fieldmasks);

        block_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], decoded_docids);
        block_encoder.decode_u8(&mut reader, &mut decoded_fieldmasks)?;
        assert_eq!(&fieldmasks[BLOCK_LEN..BLOCK_LEN * 2], decoded_fieldmasks);

        block_encoder.decode_u32(&mut reader, &mut decoded_docids[0..3])?;
        assert_eq!(
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_docids[0..3]
        );
        block_encoder.decode_u8(&mut reader, &mut decoded_fieldmasks[0..3])?;
        assert_eq!(
            &fieldmasks[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_fieldmasks[0..3]
        );

        Ok(())
    }
}
