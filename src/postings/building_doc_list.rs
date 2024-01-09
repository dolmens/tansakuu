use std::sync::Arc;

use allocator_api2::alloc::{Allocator, Global};

use crate::{
    util::{AcqRelAtomicPtr, AcqRelUsize, RelaxedU32, RelaxedU8},
    DocId, FieldMask, TermFreq, DOCLIST_BLOCK_LEN, INVALID_DOCID,
};

use super::{
    compression,
    skiplist::{BuildingSkipList, BuildingSkipListReader, BuildingSkipListWriter},
    ByteSliceList, ByteSliceReader, ByteSliceWriter, DocListBlock, DocListFormat,
};

pub struct BuildingDocList<A: Allocator = Global> {
    building_block: BuildingDocListBlock,
    flushed_count: AcqRelUsize,
    slice_list: Arc<ByteSliceList<A>>,
    building_skip_list: AcqRelAtomicPtr<BuildingSkipList>,
    doc_list_format: DocListFormat,
}

pub struct BuildingDocListBlock {
    len: AcqRelUsize,
    docids: [RelaxedU32; DOCLIST_BLOCK_LEN],
    termfreqs: Option<Box<[RelaxedU32]>>,
    fieldmasks: Option<Box<[RelaxedU8]>>,
}

pub struct DocListBlockSnapshot {
    len: usize,
    docids: Box<[DocId]>,
    termfreqs: Option<Box<[TermFreq]>>,
    fieldmasks: Option<Box<[FieldMask]>>,
}

pub struct BuildingDocListWriter<A: Allocator = Global> {
    last_docid: DocId,
    current_tf: TermFreq,
    total_tf: TermFreq,
    fieldmask: FieldMask,
    block_len: usize,
    flushed_count: usize,
    slice_writer: ByteSliceWriter<A>,
    skip_list_writer: Option<BuildingSkipListWriter<A>>,
    building_doc_list: Arc<BuildingDocList<A>>,
    doc_list_format: DocListFormat,
}

pub struct BuildingDocListReader<'a> {
    last_docid: DocId,
    read_count: usize,
    flushed_count: usize,
    block_snapshot: DocListBlockSnapshot,
    slice_reader: ByteSliceReader<'a>,
    skip_list_reader: Option<BuildingSkipListReader<'a>>,
    doc_list_format: DocListFormat,
}

impl<A: Allocator> BuildingDocList<A> {
    pub fn new(doc_list_format: DocListFormat, slice_list: Arc<ByteSliceList<A>>) -> Self {
        Self {
            building_block: BuildingDocListBlock::new(&doc_list_format),
            flushed_count: AcqRelUsize::new(0),
            slice_list,
            building_skip_list: AcqRelAtomicPtr::default(),
            doc_list_format,
        }
    }

    pub fn flushed_count(&self) -> usize {
        self.flushed_count.load()
    }

    pub fn building_block(&self) -> &BuildingDocListBlock {
        &self.building_block
    }

    pub fn slice_list(&self) -> &ByteSliceList<A> {
        &self.slice_list
    }

    pub fn doc_list_format(&self) -> &DocListFormat {
        &self.doc_list_format
    }

    pub fn serialize(&self) {}
}

impl BuildingDocListBlock {
    pub fn new(doc_list_format: &DocListFormat) -> Self {
        let docids = std::iter::repeat_with(|| RelaxedU32::new(0))
            .take(DOCLIST_BLOCK_LEN)
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap();
        let termfreqs = if doc_list_format.has_tflist() {
            Some(
                std::iter::repeat_with(|| RelaxedU32::new(0))
                    .take(DOCLIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };
        let fieldmasks = if doc_list_format.has_fieldmask() {
            Some(
                std::iter::repeat_with(|| RelaxedU8::new(0))
                    .take(DOCLIST_BLOCK_LEN)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };

        Self {
            len: AcqRelUsize::new(0),
            docids,
            termfreqs,
            fieldmasks,
        }
    }

    pub fn len(&self) -> usize {
        self.len.load()
    }

    pub fn add_docid(&self, offset: usize, docid: DocId) {
        self.docids[offset].store(docid);
    }

    pub fn add_tf(&self, offset: usize, tf: TermFreq) {
        self.termfreqs.as_ref().map(|termfreqs| {
            termfreqs[offset].store(tf);
        });
    }

    pub fn add_fieldmask(&self, offset: usize, fieldmask: FieldMask) {
        self.fieldmasks
            .as_ref()
            .map(|fieldmasks| fieldmasks[offset].store(fieldmask));
    }

    pub fn clear(&self) {
        self.len.store(0);
    }
}

impl DocListBlockSnapshot {
    fn with_capacity(capacity: usize, doc_list_format: &DocListFormat) -> Self {
        let docids = vec![0; capacity].into_boxed_slice();
        let termfreqs = if doc_list_format.has_tflist() {
            Some(vec![0; capacity].into_boxed_slice())
        } else {
            None
        };
        let fieldmasks = if doc_list_format.has_fieldmask() {
            Some(vec![0; capacity].into_boxed_slice())
        } else {
            None
        };

        Self {
            len: 0,
            docids,
            termfreqs,
            fieldmasks,
        }
    }

    fn snapshot(&mut self, building_block: &BuildingDocListBlock, block_len: usize) {
        self.docids[0..block_len]
            .iter_mut()
            .zip(building_block.docids[0..block_len].iter())
            .for_each(|(v, docid)| *v = docid.load());
        self.termfreqs.as_deref_mut().map(|termfreqs| {
            termfreqs[0..block_len]
                .iter_mut()
                .zip(building_block.termfreqs.as_ref().unwrap()[0..block_len].iter())
                .for_each(|(v, tf)| *v = tf.load())
        });
        self.fieldmasks.as_deref_mut().map(|fieldmasks| {
            fieldmasks[0..block_len]
                .iter_mut()
                .zip(building_block.fieldmasks.as_ref().unwrap()[0..block_len].iter())
                .for_each(|(v, fm)| *v = fm.load())
        });
        self.len = block_len;
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn copy_to(&self, doc_list_block: &mut DocListBlock) {
        let len = self.len;
        doc_list_block.len = len;
        if len > 0 {
            doc_list_block.docids[0..len].copy_from_slice(&self.docids[0..len]);
            if let (Some(dst_termfres), Some(src_termfreqs)) =
                (&mut doc_list_block.termfreqs, &self.termfreqs)
            {
                dst_termfres[0..len].copy_from_slice(&src_termfreqs[0..len]);
            }
            if let (Some(dst_fieldmasks), Some(src_fieldmasks)) =
                (&mut doc_list_block.fieldmasks, &self.fieldmasks)
            {
                dst_fieldmasks[0..len].copy_from_slice(&src_fieldmasks[0..len]);
            }
        }
    }
}

impl<A: Allocator> BuildingDocListWriter<A> {
    pub fn new(
        doc_list_format: DocListFormat,
        initial_slice_capacity: usize,
        allocator: A,
    ) -> Self {
        let slice_writer =
            ByteSliceWriter::with_initial_capacity_in(initial_slice_capacity, allocator);
        let slice_list = slice_writer.slice_list();
        let building_doc_list = Arc::new(BuildingDocList::new(doc_list_format.clone(), slice_list));

        Self {
            last_docid: INVALID_DOCID,
            current_tf: 0,
            total_tf: 0,
            fieldmask: 0,
            block_len: 0,
            flushed_count: 0,
            slice_writer,
            skip_list_writer: None,
            building_doc_list,
            doc_list_format,
        }
    }

    pub fn building_doc_list(&self) -> Arc<BuildingDocList<A>> {
        self.building_doc_list.clone()
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
        let building_block = &self.building_doc_list.building_block;
        building_block.add_docid(self.block_len, docid - self.last_docid);
        building_block.add_tf(self.block_len, self.current_tf);
        building_block.add_fieldmask(self.block_len, self.fieldmask);

        self.block_len += 1;
        self.building_doc_list
            .building_block
            .len
            .store(self.block_len);
        if self.block_len == DOCLIST_BLOCK_LEN {
            self.flush_building_block();
        }

        self.last_docid = docid;
        self.current_tf = 0;
    }

    fn flush_building_block(&mut self) {
        let building_block = &self.building_doc_list.building_block;
        let slice_writer = &mut self.slice_writer;
        let docids = building_block.docids[0..self.block_len]
            .iter()
            .map(|a| a.load())
            .collect::<Vec<_>>();
        compression::copy_write(&docids, slice_writer);
        if self.doc_list_format.has_tflist() {
            if let Some(termfreqs_atomics) = &building_block.termfreqs {
                let termfreqs = termfreqs_atomics[0..self.block_len]
                    .iter()
                    .map(|a| a.load())
                    .collect::<Vec<_>>();
                compression::copy_write(&termfreqs, slice_writer);
            }
        }
        if self.doc_list_format.has_fieldmask() {
            if let Some(fieldmaps_atomics) = &building_block.fieldmasks {
                let fieldmaps = fieldmaps_atomics[0..self.block_len]
                    .iter()
                    .map(|a| a.load())
                    .collect::<Vec<_>>();
                compression::copy_write(&fieldmaps, slice_writer);
            }
        }

        self.flushed_count += self.block_len;
        self.building_doc_list
            .flushed_count
            .store(self.flushed_count);

        building_block.clear();

        self.block_len = 0;
    }
}

impl<'a> BuildingDocListReader<'a> {
    pub fn open<A: Allocator>(building_doc_list: &'a BuildingDocList<A>) -> Self {
        let mut flushed_count = building_doc_list.flushed_count();
        let slice_list = building_doc_list.slice_list();
        let mut slice_reader = ByteSliceReader::open(slice_list);
        let doc_list_format = building_doc_list.doc_list_format();
        let building_block = building_doc_list.building_block();
        let block_len = building_block.len();
        let mut block_snapshot = DocListBlockSnapshot::with_capacity(block_len, doc_list_format);
        block_snapshot.snapshot(building_block, block_len);
        let flushed_count_updated = building_doc_list.flushed_count();
        if flushed_count_updated > flushed_count {
            block_snapshot.clear();
            flushed_count = flushed_count_updated;
            slice_reader = ByteSliceReader::open(slice_list);
        }
        let doc_list_format = building_doc_list.doc_list_format().clone();

        Self {
            last_docid: 0,
            read_count: 0,
            flushed_count,
            block_snapshot,
            slice_reader,
            skip_list_reader: None,
            doc_list_format,
        }
    }

    pub fn decode_one_block(
        &mut self,
        start_docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> bool {
        if self.eof() {
            return false;
        }

        if self.read_count < self.flushed_count {
            if self.decode_one_flushed_block(start_docid, doc_list_block) {
                return true;
            }
        }

        if !self.block_snapshot.is_empty() {
            self.block_snapshot.copy_to(doc_list_block);
            doc_list_block.decode(self.last_docid);
            self.last_docid = doc_list_block.last_docid();
            self.read_count += self.block_snapshot.len;
            return doc_list_block.last_docid() >= start_docid;
        }

        return false;
    }

    fn decode_one_flushed_block(
        &mut self,
        start_docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> bool {
        while self.read_count < self.flushed_count {
            let compressed = (self.flushed_count - self.read_count) >= DOCLIST_BLOCK_LEN;
            if compressed {
                doc_list_block.len = DOCLIST_BLOCK_LEN;
                compression::copy_read(&mut self.slice_reader, &mut doc_list_block.docids);
                if self.doc_list_format.has_tflist() {
                    if let Some(termfreqs) = doc_list_block.termfreqs.as_deref_mut() {
                        compression::copy_read(&mut self.slice_reader, termfreqs);
                    }
                }
                if self.doc_list_format.has_fieldmask() {
                    if let Some(fieldmasks) = doc_list_block.fieldmasks.as_deref_mut() {
                        compression::copy_read(&mut self.slice_reader, fieldmasks);
                    } else {
                        assert!(false);
                    }
                }
            } else {
                let block_len = self.flushed_count - self.read_count;
                doc_list_block.len = block_len;
                compression::copy_read(
                    &mut self.slice_reader,
                    &mut doc_list_block.docids[0..block_len],
                );
                if self.doc_list_format.has_tflist() {
                    if let Some(termfreqs) = doc_list_block.termfreqs.as_deref_mut() {
                        compression::copy_read(
                            &mut self.slice_reader,
                            &mut termfreqs[0..block_len],
                        );
                    }
                }
                if self.doc_list_format.has_fieldmask() {
                    if let Some(fieldmasks) = doc_list_block.fieldmasks.as_deref_mut() {
                        compression::copy_read(
                            &mut self.slice_reader,
                            &mut fieldmasks[0..block_len],
                        );
                    } else {
                        assert!(false);
                    }
                }
            }
            doc_list_block.decode(self.last_docid);
            self.last_docid = doc_list_block.last_docid();
            self.read_count += doc_list_block.len;
            if doc_list_block.last_docid() >= start_docid {
                return true;
            }
        }

        return false;
    }

    pub fn total_count(&self) -> usize {
        self.flushed_count + self.block_snapshot.len
    }

    pub fn read_count(&self) -> usize {
        self.read_count
    }

    pub fn eof(&self) -> bool {
        self.read_count == self.flushed_count + self.block_snapshot.len
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use allocator_api2::alloc::Global;

    use crate::{
        postings::{DocListBlock, DocListFormat},
        DocId, DOCLIST_BLOCK_LEN,
    };

    use super::{BuildingDocListReader, BuildingDocListWriter};

    #[test]
    fn test_basic() {
        let doc_list_format = DocListFormat::new(true, false, false);
        let mut doc_list_writer = BuildingDocListWriter::new(doc_list_format.clone(), 1024, Global);
        let building_doc_list = doc_list_writer.building_doc_list();
        let mut doc_list_block = DocListBlock::new(&doc_list_format);
        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(!doc_list_reader.decode_one_block(0, &mut doc_list_block));
        assert!(!doc_list_reader.decode_one_block(0, &mut doc_list_block));

        doc_list_writer.add_pos(1);
        doc_list_writer.add_pos(2);
        doc_list_writer.end_doc(0 * 3 + 0 % 2);

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(doc_list_reader.decode_one_block(0, &mut doc_list_block));
        assert_eq!(doc_list_block.len, 1);
        assert_eq!(doc_list_block.first_docid(), 0 * 3 + 0 % 2);
        assert_eq!(doc_list_block.last_docid(), 0 * 3 + 0 % 2);
        assert!(!doc_list_reader.decode_one_block(1, &mut doc_list_block));

        doc_list_writer.add_pos(1);
        doc_list_writer.end_doc(1 * 3 + 1 % 2);

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(doc_list_reader.decode_one_block(0, &mut doc_list_block));
        assert_eq!(doc_list_block.len, 2);
        assert_eq!(doc_list_block.first_docid(), 0 * 3 + 0 % 2);
        assert_eq!(doc_list_block.last_docid(), 1 * 3 + 1 % 2);
        assert!(!doc_list_reader.decode_one_block(3, &mut doc_list_block));

        doc_list_writer.add_pos(1);
        doc_list_writer.add_pos(2);
        doc_list_writer.add_pos(3);
        doc_list_writer.end_doc(2 * 3 + 2 % 2);

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(doc_list_reader.decode_one_block(0, &mut doc_list_block));
        assert_eq!(doc_list_block.len, 3);
        assert_eq!(doc_list_block.first_docid(), 0 * 3 + 0 % 2);
        assert_eq!(doc_list_block.docids[1], 1 * 3 + 1 % 2);
        assert_eq!(doc_list_block.last_docid(), 2 * 3 + 2 % 2);
        assert!(!doc_list_reader.decode_one_block(6, &mut doc_list_block));

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(doc_list_reader.decode_one_block(5, &mut doc_list_block));
        assert_eq!(doc_list_block.len, 3);
        assert_eq!(doc_list_block.first_docid(), 0 * 3 + 0 % 2);
        assert_eq!(doc_list_block.docids[1], 1 * 3 + 1 % 2);
        assert_eq!(doc_list_block.last_docid(), 2 * 3 + 2 % 2);
        let last_docid = doc_list_block.last_docid();
        assert!(!doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(!doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));

        for i in 3..DOCLIST_BLOCK_LEN {
            doc_list_writer.add_pos(1);
            doc_list_writer.end_doc((i * 3 + i % 2) as u32);
        }

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(doc_list_reader.decode_one_block(100, &mut doc_list_block));
        assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
        for i in 0..DOCLIST_BLOCK_LEN {
            assert_eq!(doc_list_block.docids[i], (i * 3 + i % 2) as DocId);
        }
        let last_docid = doc_list_block.last_docid();
        assert!(!doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));

        doc_list_writer.add_pos(1);
        doc_list_writer.add_pos(2);
        doc_list_writer.end_doc((DOCLIST_BLOCK_LEN * 3 + DOCLIST_BLOCK_LEN % 2) as DocId);

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(doc_list_reader.decode_one_block(100, &mut doc_list_block));
        assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
        for i in 0..DOCLIST_BLOCK_LEN {
            assert_eq!(doc_list_block.docids[i], (i * 3 + i % 2) as DocId);
        }
        let last_docid = doc_list_block.last_docid();
        assert!(doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));
        assert_eq!(doc_list_block.len, 1);
        assert_eq!(
            doc_list_block.first_docid(),
            (DOCLIST_BLOCK_LEN * 3 + DOCLIST_BLOCK_LEN % 2) as DocId
        );

        let last_docid = doc_list_block.last_docid();
        assert!(!doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));

        for i in 1..DOCLIST_BLOCK_LEN + 2 {
            doc_list_writer.add_pos(1);
            let docid = (i + DOCLIST_BLOCK_LEN) * 3 + (i + DOCLIST_BLOCK_LEN) % 2;
            doc_list_writer.end_doc(docid as DocId);
        }

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        assert!(doc_list_reader.decode_one_block(100, &mut doc_list_block));
        assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
        for i in 0..DOCLIST_BLOCK_LEN {
            assert_eq!(doc_list_block.docids[i], (i * 3 + i % 2) as DocId);
        }

        let last_docid = doc_list_block.last_docid();
        assert!(doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));
        assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
        for i in 0..DOCLIST_BLOCK_LEN {
            let docid = (i + DOCLIST_BLOCK_LEN) * 3 + (i + DOCLIST_BLOCK_LEN) % 2;
            assert_eq!(doc_list_block.docids[i], docid as DocId);
        }

        let last_docid = doc_list_block.last_docid();
        assert!(doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));
        assert_eq!(doc_list_block.len, 2);
        assert_eq!(
            doc_list_block.first_docid(),
            ((DOCLIST_BLOCK_LEN * 2) * 3 + (DOCLIST_BLOCK_LEN * 2) % 2) as DocId
        );
        assert_eq!(
            doc_list_block.last_docid(),
            ((DOCLIST_BLOCK_LEN * 2 + 1) * 3 + (DOCLIST_BLOCK_LEN * 2 + 1) % 2) as DocId
        );

        let last_docid = doc_list_block.last_docid();
        assert!(!doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        let last_docid = (DOCLIST_BLOCK_LEN * 3 + DOCLIST_BLOCK_LEN % 2) as DocId;
        assert!(doc_list_reader.decode_one_block(last_docid, &mut doc_list_block));
        assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
        for i in 0..DOCLIST_BLOCK_LEN {
            let docid = ((DOCLIST_BLOCK_LEN + i) * 3 + (DOCLIST_BLOCK_LEN + i) % 2) as DocId;
            assert_eq!(doc_list_block.docids[i], docid);
        }

        let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
        let last_docid = (DOCLIST_BLOCK_LEN * 2 * 3 + DOCLIST_BLOCK_LEN * 2 % 2) as DocId;
        assert!(doc_list_reader.decode_one_block(last_docid, &mut doc_list_block));
        assert_eq!(doc_list_block.len, 2);
        assert_eq!(
            doc_list_block.first_docid(),
            ((DOCLIST_BLOCK_LEN * 2) * 3 + (DOCLIST_BLOCK_LEN * 2) % 2) as DocId
        );
        assert_eq!(
            doc_list_block.last_docid(),
            ((DOCLIST_BLOCK_LEN * 2 + 1) * 3 + (DOCLIST_BLOCK_LEN * 2 + 1) % 2) as DocId
        );
    }

    #[test]
    fn test_multithreads_sync() {
        let (w_sender, r_receiver) = mpsc::channel();
        let (r_sender, w_receiver) = mpsc::channel();

        let doc_list_format = DocListFormat::new(true, false, false);
        let mut doc_list_writer = BuildingDocListWriter::new(doc_list_format.clone(), 1024, Global);
        let building_doc_list = doc_list_writer.building_doc_list();

        let w = thread::spawn(move || {
            doc_list_writer.add_pos(1);
            doc_list_writer.add_pos(2);

            w_receiver.recv().unwrap();
            doc_list_writer.end_doc(0 * 3 + 0 % 2);
            w_sender.send(0).unwrap();

            doc_list_writer.add_pos(1);
            w_receiver.recv().unwrap();
            doc_list_writer.end_doc(1 * 3 + 1 % 2);
            w_sender.send(0).unwrap();

            doc_list_writer.add_pos(1);
            doc_list_writer.add_pos(2);
            doc_list_writer.add_pos(3);
            w_receiver.recv().unwrap();
            doc_list_writer.end_doc(2 * 3 + 2 % 2);
            w_sender.send(0).unwrap();

            w_receiver.recv().unwrap();
            for i in 3..DOCLIST_BLOCK_LEN {
                doc_list_writer.add_pos(1);
                doc_list_writer.end_doc((i * 3 + i % 2) as u32);
            }
            w_sender.send(0).unwrap();

            doc_list_writer.add_pos(1);
            doc_list_writer.add_pos(2);
            w_receiver.recv().unwrap();
            doc_list_writer.end_doc((DOCLIST_BLOCK_LEN * 3 + DOCLIST_BLOCK_LEN % 2) as DocId);
            w_sender.send(0).unwrap();

            w_receiver.recv().unwrap();
            for i in 1..DOCLIST_BLOCK_LEN + 2 {
                doc_list_writer.add_pos(1);
                let docid = (i + DOCLIST_BLOCK_LEN) * 3 + (i + DOCLIST_BLOCK_LEN) % 2;
                doc_list_writer.end_doc(docid as DocId);
            }
            w_sender.send(0).unwrap();
        });

        let r = thread::spawn(move || {
            let mut doc_list_block = DocListBlock::new(&doc_list_format);

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            assert!(!doc_list_reader.decode_one_block(0, &mut doc_list_block));
            assert!(!doc_list_reader.decode_one_block(0, &mut doc_list_block));

            r_sender.send(0).unwrap();
            r_receiver.recv().unwrap();

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            assert!(doc_list_reader.decode_one_block(0, &mut doc_list_block));
            assert_eq!(doc_list_block.len, 1);
            assert_eq!(doc_list_block.first_docid(), 0 * 3 + 0 % 2);
            assert_eq!(doc_list_block.last_docid(), 0 * 3 + 0 % 2);
            assert!(!doc_list_reader.decode_one_block(1, &mut doc_list_block));

            r_sender.send(0).unwrap();
            r_receiver.recv().unwrap();

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            assert!(doc_list_reader.decode_one_block(0, &mut doc_list_block));
            assert_eq!(doc_list_block.len, 2);
            assert_eq!(doc_list_block.first_docid(), 0 * 3 + 0 % 2);
            assert_eq!(doc_list_block.last_docid(), 1 * 3 + 1 % 2);
            assert!(!doc_list_reader.decode_one_block(3, &mut doc_list_block));

            r_sender.send(0).unwrap();
            r_receiver.recv().unwrap();

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            assert!(doc_list_reader.decode_one_block(0, &mut doc_list_block));
            assert_eq!(doc_list_block.len, 3);
            assert_eq!(doc_list_block.first_docid(), 0 * 3 + 0 % 2);
            assert_eq!(doc_list_block.docids[1], 1 * 3 + 1 % 2);
            assert_eq!(doc_list_block.last_docid(), 2 * 3 + 2 % 2);
            assert!(!doc_list_reader.decode_one_block(6, &mut doc_list_block));

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            assert!(doc_list_reader.decode_one_block(5, &mut doc_list_block));
            assert_eq!(doc_list_block.len, 3);
            assert_eq!(doc_list_block.first_docid(), 0 * 3 + 0 % 2);
            assert_eq!(doc_list_block.docids[1], 1 * 3 + 1 % 2);
            assert_eq!(doc_list_block.last_docid(), 2 * 3 + 2 % 2);
            assert!(!doc_list_reader.decode_one_block(6, &mut doc_list_block));

            r_sender.send(0).unwrap();
            r_receiver.recv().unwrap();

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            assert!(doc_list_reader.decode_one_block(100, &mut doc_list_block));
            assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
            for i in 0..DOCLIST_BLOCK_LEN {
                assert_eq!(doc_list_block.docids[i], (i * 3 + i % 2) as DocId);
            }
            let last_docid = doc_list_block.last_docid();
            assert!(!doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));

            r_sender.send(0).unwrap();
            r_receiver.recv().unwrap();

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            assert!(doc_list_reader.decode_one_block(100, &mut doc_list_block));
            assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
            for i in 0..DOCLIST_BLOCK_LEN {
                assert_eq!(doc_list_block.docids[i], (i * 3 + i % 2) as DocId);
            }
            let last_docid = doc_list_block.last_docid();
            assert!(doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));
            assert_eq!(doc_list_block.len, 1);
            assert_eq!(
                doc_list_block.first_docid(),
                (DOCLIST_BLOCK_LEN * 3 + DOCLIST_BLOCK_LEN % 2) as DocId
            );

            let last_docid = doc_list_block.last_docid();
            assert!(!doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));

            r_sender.send(0).unwrap();
            r_receiver.recv().unwrap();

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            assert!(doc_list_reader.decode_one_block(100, &mut doc_list_block));
            assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
            for i in 0..DOCLIST_BLOCK_LEN {
                assert_eq!(doc_list_block.docids[i], (i * 3 + i % 2) as DocId);
            }

            let last_docid = doc_list_block.last_docid();
            assert!(doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));
            assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
            for i in 0..DOCLIST_BLOCK_LEN {
                let docid = (i + DOCLIST_BLOCK_LEN) * 3 + (i + DOCLIST_BLOCK_LEN) % 2;
                assert_eq!(doc_list_block.docids[i], docid as DocId);
            }

            let last_docid = doc_list_block.last_docid();
            assert!(doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));
            assert_eq!(doc_list_block.len, 2);
            assert_eq!(
                doc_list_block.first_docid(),
                ((DOCLIST_BLOCK_LEN * 2) * 3 + (DOCLIST_BLOCK_LEN * 2) % 2) as DocId
            );
            assert_eq!(
                doc_list_block.last_docid(),
                ((DOCLIST_BLOCK_LEN * 2 + 1) * 3 + (DOCLIST_BLOCK_LEN * 2 + 1) % 2) as DocId
            );

            let last_docid = doc_list_block.last_docid();
            assert!(!doc_list_reader.decode_one_block(last_docid + 1, &mut doc_list_block));

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            let last_docid = (DOCLIST_BLOCK_LEN * 3 + DOCLIST_BLOCK_LEN % 2) as DocId;
            assert!(doc_list_reader.decode_one_block(last_docid, &mut doc_list_block));
            assert_eq!(doc_list_block.len, DOCLIST_BLOCK_LEN);
            for i in 0..DOCLIST_BLOCK_LEN {
                let docid = ((DOCLIST_BLOCK_LEN + i) * 3 + (DOCLIST_BLOCK_LEN + i) % 2) as DocId;
                assert_eq!(doc_list_block.docids[i], docid);
            }

            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            let last_docid = (DOCLIST_BLOCK_LEN * 2 * 3 + DOCLIST_BLOCK_LEN * 2 % 2) as DocId;
            assert!(doc_list_reader.decode_one_block(last_docid, &mut doc_list_block));
            assert_eq!(doc_list_block.len, 2);
            assert_eq!(
                doc_list_block.first_docid(),
                ((DOCLIST_BLOCK_LEN * 2) * 3 + (DOCLIST_BLOCK_LEN * 2) % 2) as DocId
            );
            assert_eq!(
                doc_list_block.last_docid(),
                ((DOCLIST_BLOCK_LEN * 2 + 1) * 3 + (DOCLIST_BLOCK_LEN * 2 + 1) % 2) as DocId
            );
        });

        w.join().unwrap();
        r.join().unwrap();
    }

    #[test]
    fn test_multithreads_random() {
        let doc_list_format = DocListFormat::new(true, false, false);
        let mut doc_list_writer = BuildingDocListWriter::new(doc_list_format.clone(), 1024, Global);
        let building_doc_list = doc_list_writer.building_doc_list();

        let w = thread::spawn(move || {
            for i in 0..DOCLIST_BLOCK_LEN * 2 + 2 {
                doc_list_writer.add_pos(1);
                let docid = (i * 3 + i % 2) as DocId;
                thread::yield_now();
                doc_list_writer.end_doc(docid);
            }
        });

        let r = thread::spawn(move || loop {
            let mut doc_list_block = DocListBlock::new(&doc_list_format);
            let mut doc_list_reader = BuildingDocListReader::open(&building_doc_list);
            let mut total_count = doc_list_reader.total_count();
            let mut last_docid = 0;
            while total_count > 0 {
                let read_count = doc_list_reader.read_count();
                assert!(doc_list_reader.decode_one_block(last_docid, &mut doc_list_block));
                let expect_block_len = std::cmp::min(DOCLIST_BLOCK_LEN, total_count);
                assert_eq!(doc_list_block.len, expect_block_len);
                for i in 0..expect_block_len {
                    let docid = ((i + read_count) * 3 + (i + read_count) % 2) as DocId;
                    assert_eq!(doc_list_block.docids[i], docid);
                }
                last_docid = doc_list_block.last_docid() + 1;
                total_count -= expect_block_len;
            }

            let total_count = doc_list_reader.total_count();
            if total_count == DOCLIST_BLOCK_LEN * 2 + 2 {
                break;
            }

            thread::sleep(Duration::from_micros(10));
        });

        w.join().unwrap();
        r.join().unwrap();
    }
}
