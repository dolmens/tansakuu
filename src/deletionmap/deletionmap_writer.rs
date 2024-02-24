use std::{io::Write, path::PathBuf};

use tantivy_common::TerminatingWrite;

use crate::{
    table::{SegmentId, TableData},
    util::{FixedSizeBitset, FixedSizeBitsetWriter, MutableBitset},
    Directory, DocId,
};

use super::DeletionMap;

pub struct DeletionMapWriter {
    segments: Vec<DeletionMapSegmentWriter>,
    global_writer: FixedSizeBitsetWriter,
}

pub struct DeletionMapSegmentWriter {
    base_docid: DocId,
    doc_count: usize,
    bitset: FixedSizeBitsetWriter,
    segment_id: SegmentId,
}

impl DeletionMapWriter {
    pub fn new(table_data: &mut TableData) -> Self {
        let mut segments = vec![];
        for seg in table_data.persistent_segments() {
            let bitset =
                FixedSizeBitsetWriter::new_with_immutable_bitset(seg.data().deletionmap().bitset());
            segments.push(DeletionMapSegmentWriter {
                base_docid: seg.meta().base_docid(),
                doc_count: seg.meta().doc_count(),
                bitset,
                segment_id: seg.meta().segment_id().clone(),
            });
        }
        for seg in table_data.building_segments() {
            if seg.is_dumping() {
                let bitset = FixedSizeBitsetWriter::new_with_expandable_bitset(
                    seg.data().deletionmap().bitset(),
                );
                segments.push(DeletionMapSegmentWriter {
                    base_docid: seg.meta().base_docid(),
                    doc_count: seg.meta().doc_count(),
                    bitset,
                    segment_id: seg.meta().segment_id().clone(),
                });
            }
        }

        let fixed_doc_count = segments.iter().map(|seg| seg.doc_count).sum::<usize>();
        let mut global_bitset = MutableBitset::with_capacity(fixed_doc_count);
        for seg in &segments {
            let bitset = seg.bitset();
            let data: Vec<_> = bitset.as_loaded_words().collect();
            global_bitset.copy_data_at(&data, seg.base_docid as usize, seg.doc_count);
        }
        let global_writer = FixedSizeBitsetWriter::new(global_bitset.data());
        let deletionmap = DeletionMap::new(global_writer.bitset().into());
        table_data.set_deletionmap(deletionmap);

        Self {
            segments,
            global_writer,
        }
    }

    pub fn delete_document(&mut self, docid: DocId) {
        self.global_writer.insert(docid as usize);
        for seg in &mut self.segments {
            if docid < seg.base_docid + (seg.doc_count as DocId) {
                seg.delete_document(docid - seg.base_docid);
            }
        }
    }

    pub fn save(&self, directory: &dyn Directory) {
        for seg in &self.segments {
            seg.save(directory);
        }
    }
}

impl DeletionMapSegmentWriter {
    pub fn delete_document(&mut self, docid_in_segment: DocId) {
        self.bitset.insert(docid_in_segment as usize);
    }

    pub fn bitset(&self) -> FixedSizeBitset {
        self.bitset.bitset()
    }

    pub fn save(&self, directory: &dyn Directory) {
        let path = PathBuf::from("deletionmap").join(self.segment_id.as_str());
        let mut writer = directory.open_write(&path).unwrap();
        for word in self.bitset.as_loaded_words() {
            writer.write_all(&word.to_le_bytes()).unwrap();
        }
        writer.terminate().unwrap();
    }
}
