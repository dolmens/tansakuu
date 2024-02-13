use std::{io, sync::Arc};

use allocator_api2::alloc::{Allocator, Global};

use crate::DocId;

use super::{
    doc_list_encoder::{BuildingDocListBlock, DocListBlockSnapshot, DocListFlushInfo},
    doc_list_encoder_builder,
    skip_list::{BuildingSkipList, BuildingSkipListReader, BuildingSkipListWriter},
    ByteSliceList, ByteSliceReader, ByteSliceWriter, DocListBlock, DocListDecode, DocListDecoder,
    DocListEncode, DocListEncoder, DocListFormat,
};

#[derive(Clone)]
pub struct BuildingDocList<A: Allocator = Global> {
    flush_info: Arc<DocListFlushInfo>,
    building_block: Arc<BuildingDocListBlock>,
    byte_slice_list: Arc<ByteSliceList<A>>,
    building_skip_list: BuildingSkipList<A>,
    doc_list_format: DocListFormat,
}

pub struct BuildingDocListEncoder<A: Allocator = Global> {
    doc_list_encoder: DocListEncoder<ByteSliceWriter<A>, BuildingSkipListWriter<A>>,
    building_doc_list: BuildingDocList<A>,
}

pub struct BuildingDocListDecoder<'a> {
    read_count: usize,
    block_offset: usize,
    flushed_count: usize,
    building_block_snapshot: DocListBlockSnapshot,
    doc_list_decoder: DocListDecoder<ByteSliceReader<'a>, BuildingSkipListReader<'a>>,
}

impl<A: Allocator + Clone + Default> BuildingDocListEncoder<A> {
    pub fn new(doc_list_format: DocListFormat, initial_slice_capacity: usize) -> Self {
        Self::new_in(doc_list_format, initial_slice_capacity, A::default())
    }
}

impl<A: Allocator + Clone> BuildingDocListEncoder<A> {
    pub fn new_in(
        doc_list_format: DocListFormat,
        initial_slice_capacity: usize,
        allocator: A,
    ) -> Self {
        let byte_slice_writer =
            ByteSliceWriter::with_initial_capacity_in(initial_slice_capacity, allocator.clone());
        let byte_slice_list = byte_slice_writer.byte_slice_list();
        let skip_list_format = doc_list_format.skip_list_format().clone();

        let skip_list_writer = BuildingSkipListWriter::new_in(
            skip_list_format,
            initial_slice_capacity,
            allocator.clone(),
        );
        let building_skip_list = skip_list_writer.building_skip_list().clone();

        let doc_list_encoder = doc_list_encoder_builder(doc_list_format.clone())
            .with_writer(byte_slice_writer)
            .with_skip_list_writer(skip_list_writer)
            .build();
        let flush_info = doc_list_encoder.flush_info().clone();
        let building_block = doc_list_encoder.building_block().clone();

        let building_doc_list = BuildingDocList {
            flush_info,
            building_block,
            byte_slice_list,
            building_skip_list,
            doc_list_format,
        };

        Self {
            doc_list_encoder,
            building_doc_list,
        }
    }

    pub fn building_doc_list(&self) -> &BuildingDocList<A> {
        &self.building_doc_list
    }
}

impl<A: Allocator> DocListEncode for BuildingDocListEncoder<A> {
    fn add_pos(&mut self, field: usize) -> io::Result<()> {
        self.doc_list_encoder.add_pos(field)
    }

    fn end_doc(&mut self, docid: DocId) -> io::Result<()> {
        self.doc_list_encoder.end_doc(docid)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.doc_list_encoder.flush()
    }

    fn df(&self) -> usize {
        self.doc_list_encoder.df()
    }

    fn written_bytes(&self) -> (usize, usize) {
        self.doc_list_encoder.written_bytes()
    }
}

impl<'a> BuildingDocListDecoder<'a> {
    pub fn open<A: Allocator>(building_doc_list: &'a BuildingDocList<A>) -> Self {
        let flush_info = building_doc_list.flush_info.load();
        let byte_slice_list = building_doc_list.byte_slice_list.as_ref();
        let building_block = building_doc_list.building_block.as_ref();
        let doc_list_format = building_doc_list.doc_list_format.clone();
        let mut flushed_count = flush_info.flushed_count();
        let mut byte_slice_reader = if flushed_count == 0 {
            ByteSliceReader::empty()
        } else {
            ByteSliceReader::open(byte_slice_list)
        };
        let mut building_block_snapshot = building_block.snapshot(flush_info.buffer_len());
        let flushed_count_updated = building_doc_list.flush_info.load().flushed_count();
        if flushed_count < flushed_count_updated {
            building_block_snapshot.clear();
            flushed_count = flushed_count_updated;
            byte_slice_reader = ByteSliceReader::open(byte_slice_list);
        }

        let skip_list_reader = BuildingSkipListReader::open(&building_doc_list.building_skip_list);

        let doc_list_decoder = DocListDecoder::open_with_skip_list_reader(
            doc_list_format,
            flushed_count,
            byte_slice_reader,
            skip_list_reader,
        );

        Self {
            read_count: 0,
            block_offset: 0,
            flushed_count,
            building_block_snapshot,
            doc_list_decoder,
        }
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.flushed_count + self.building_block_snapshot.len()
    }

    pub fn doc_count(&self) -> usize {
        self.flushed_count + self.building_block_snapshot.len()
    }

    pub fn read_count(&self) -> usize {
        self.read_count
    }
}

impl<'a> DocListDecode for BuildingDocListDecoder<'a> {
    fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        if self.eof() {
            return Ok(false);
        }

        if self.read_count < self.flushed_count {
            if self
                .doc_list_decoder
                .decode_doc_buffer(docid, doc_list_block)?
            {
                self.block_offset = self.read_count;
                self.read_count += doc_list_block.len;
                return Ok(true);
            }
        }

        self.read_count = self.flushed_count;
        self.block_offset = self.read_count;

        let len = self.building_block_snapshot.len();
        if len == 0 {
            return Ok(false);
        }

        self.read_count += len;

        let mut last_docid = self.doc_list_decoder.last_docid();
        let base_docid = last_docid;

        doc_list_block.len = len;
        doc_list_block.docids[0..len]
            .copy_from_slice(self.building_block_snapshot.docids().unwrap());

        for i in 0..len {
            last_docid += doc_list_block.docids[i];
        }
        if last_docid < docid {
            return Ok(false);
        }

        doc_list_block.base_docid = base_docid;
        doc_list_block.last_docid = last_docid;
        doc_list_block.base_ttf = self.doc_list_decoder.last_ttf();

        Ok(true)
    }

    fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        if self.doc_list_decoder.doc_list_format().has_tflist() {
            if self.block_offset < self.flushed_count {
                return self.doc_list_decoder.decode_tf_buffer(doc_list_block);
            }
            let termfreqs = doc_list_block.termfreqs.as_deref_mut().unwrap();
            let snapshot = self.building_block_snapshot.termfreqs().unwrap();
            let len = snapshot.len();
            termfreqs[0..len].copy_from_slice(snapshot);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn decode_fieldmask_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        if self.doc_list_decoder.doc_list_format().has_fieldmask() {
            if self.block_offset < self.flushed_count {
                return self
                    .doc_list_decoder
                    .decode_fieldmask_buffer(doc_list_block);
            }
            let fieldmask = doc_list_block.fieldmasks.as_deref_mut().unwrap();
            let snapshot = self.building_block_snapshot.fieldmasks().unwrap();
            let len = snapshot.len();
            fieldmask[0..len].copy_from_slice(snapshot);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, thread};

    use crate::{
        postings::{
            building_doc_list::{BuildingDocListDecoder, BuildingDocListEncoder},
            DocListBlock, DocListDecode, DocListEncode, DocListFormat,
        },
        DocId, DOC_LIST_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let doc_list_format = DocListFormat::builder().with_tflist().build();
        let mut doc_list_encoder: BuildingDocListEncoder =
            BuildingDocListEncoder::new(doc_list_format.clone(), 1024);
        let building_doc_list = doc_list_encoder.building_doc_list().clone();
        let mut doc_list_block = DocListBlock::new(&doc_list_format);
        let mut doc_list_decoder = BuildingDocListDecoder::open(&building_doc_list);
        assert!(doc_list_decoder.eof());
        assert_eq!(doc_list_decoder.doc_count(), 0);
        assert_eq!(doc_list_decoder.read_count, 0);
        assert!(!doc_list_decoder.decode_one_block(0, &mut doc_list_block)?);
        assert!(doc_list_decoder.eof());
        assert_eq!(doc_list_decoder.doc_count(), 0);
        assert_eq!(doc_list_decoder.read_count, 0);

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

        let mut doc_list_decoder = BuildingDocListDecoder::open(&building_doc_list);
        assert!(!doc_list_decoder.eof());
        assert_eq!(doc_list_decoder.doc_count(), 1);
        assert_eq!(doc_list_decoder.read_count, 0);
        assert!(doc_list_decoder.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[0]);
        assert_eq!(doc_list_block.len, 1);
        assert_eq!(doc_list_block.docids[0], docids[0]);
        assert_eq!(doc_list_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);

        assert!(doc_list_decoder.eof());
        assert_eq!(doc_list_decoder.doc_count(), 1);
        assert_eq!(doc_list_decoder.read_count, 1);

        assert!(!doc_list_decoder.decode_one_block(docids[0], &mut doc_list_block)?);

        for _ in 0..termfreqs[1] {
            doc_list_encoder.add_pos(0)?;
        }
        doc_list_encoder.end_doc(docids[1])?;

        let mut posting_reader = BuildingDocListDecoder::open(&building_doc_list);
        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[1]);
        assert_eq!(doc_list_block.len, 2);
        assert_eq!(doc_list_block.docids[0], docids_deltas[0]);
        assert_eq!(doc_list_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);
        assert_eq!(doc_list_block.docids[1], docids_deltas[1]);
        assert_eq!(doc_list_block.termfreqs.as_ref().unwrap()[1], termfreqs[1]);

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        for i in 2..BLOCK_LEN {
            for _ in 0..termfreqs[i] {
                doc_list_encoder.add_pos(0)?;
            }
            doc_list_encoder.end_doc(docids[i])?;
        }

        let mut posting_reader = BuildingDocListDecoder::open(&building_doc_list);
        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);
        assert_eq!(doc_list_block.docids, &docids_deltas[0..BLOCK_LEN]);
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                doc_list_encoder.add_pos(0)?;
            }
            doc_list_encoder.end_doc(docids[i + BLOCK_LEN])?;
        }

        let mut posting_reader = BuildingDocListDecoder::open(&building_doc_list);

        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);
        assert_eq!(doc_list_block.docids, &docids_deltas[0..BLOCK_LEN]);
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);
        assert_eq!(
            doc_list_block.docids,
            &docids_deltas[BLOCK_LEN..BLOCK_LEN * 2]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(doc_list_block.len, 3);
        assert_eq!(
            &doc_list_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        // skip one block

        let mut posting_reader = BuildingDocListDecoder::open(&building_doc_list);

        assert!(posting_reader.decode_one_block(docids[BLOCK_LEN - 1] + 1, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);
        assert_eq!(
            doc_list_block.docids,
            &docids_deltas[BLOCK_LEN..BLOCK_LEN * 2]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(doc_list_block.len, 3);
        assert_eq!(
            &doc_list_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        // skip two blocks

        let mut posting_reader = BuildingDocListDecoder::open(&building_doc_list);

        assert!(
            posting_reader.decode_one_block(docids[BLOCK_LEN * 2 - 1] + 1, &mut doc_list_block)?
        );
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(doc_list_block.len, 3);
        assert_eq!(
            &doc_list_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        // skip to end

        let mut posting_reader = BuildingDocListDecoder::open(&building_doc_list);

        assert!(!posting_reader
            .decode_one_block(docids.last().cloned().unwrap() + 1, &mut doc_list_block)?);

        Ok(())
    }

    #[test]
    fn test_multithread() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let doc_list_format = DocListFormat::builder().with_tflist().build();
        let mut doc_list_encoder: BuildingDocListEncoder =
            BuildingDocListEncoder::new(doc_list_format.clone(), 1024);
        let building_doc_list = doc_list_encoder.building_doc_list().clone();

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

        thread::scope(|scope| {
            let w = scope.spawn(move || {
                for i in 0..BLOCK_LEN * 2 + 3 {
                    for _ in 0..termfreqs[i] {
                        doc_list_encoder.add_pos(0).unwrap();
                    }
                    doc_list_encoder.end_doc(docids[i]).unwrap();
                    thread::yield_now();
                }
            });

            let r = scope.spawn(move || loop {
                let mut doc_list_block = DocListBlock::new(&doc_list_format);
                let mut posting_reader = BuildingDocListDecoder::open(&building_doc_list);
                let mut query_docid = 0;
                let mut offset = 0;
                loop {
                    if posting_reader
                        .decode_one_block(query_docid, &mut doc_list_block)
                        .unwrap()
                    {
                        let block_len = doc_list_block.len;
                        let prev_docid = if offset > 0 { docids[offset - 1] } else { 0 };
                        assert_eq!(doc_list_block.base_docid, prev_docid);
                        assert_eq!(doc_list_block.last_docid, docids[offset + block_len - 1]);
                        assert_eq!(
                            &doc_list_block.docids[0..block_len],
                            &docids_deltas[offset..offset + block_len]
                        );

                        assert_eq!(
                            &doc_list_block.termfreqs.as_ref().unwrap()[0..block_len],
                            &termfreqs[offset..offset + block_len]
                        );
                        query_docid = doc_list_block.last_docid + 1;
                        offset += block_len;
                    } else {
                        break;
                    }
                }
                if posting_reader.doc_count() == BLOCK_LEN * 2 + 3 {
                    break;
                }
                thread::yield_now();
            });

            w.join().unwrap();
            r.join().unwrap();
        });

        Ok(())
    }
}
