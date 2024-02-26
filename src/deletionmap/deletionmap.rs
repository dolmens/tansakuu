use tantivy_common::HasLen;

use crate::{
    table::SegmentId,
    util::{Bitset, ImmutableBitset, MutableBitset},
    Directory, DocId,
};

use std::path::PathBuf;

#[derive(Clone)]
pub struct ImmutableDeletionMap {
    doc_count: usize,
    bitset: ImmutableBitset,
}

#[derive(Clone)]
pub struct MutableDeletionMap {
    doc_count: usize,
    bitset: MutableBitset,
}

impl Into<ImmutableDeletionMap> for MutableDeletionMap {
    fn into(self) -> ImmutableDeletionMap {
        ImmutableDeletionMap {
            doc_count: self.doc_count,
            bitset: self.bitset.into(),
        }
    }
}

#[derive(Clone)]
pub struct DeletionMap {
    doc_count: usize,
    bitset: Bitset,
}

impl ImmutableDeletionMap {
    pub fn load(directory: &dyn Directory, segment_id: SegmentId, doc_count: usize) -> Self {
        let deletionmap_path = PathBuf::from("deletionmap").join(segment_id.as_str());
        if directory.exists(&deletionmap_path).unwrap() {
            let deletionmap_data = directory.open_read(&deletionmap_path).unwrap();
            if deletionmap_data.len() % 8 != 0 || deletionmap_data.len() * 8 < doc_count {
                let mut deletionmap_bytes = deletionmap_data.read_bytes().unwrap();
                let words: Vec<_> = (0..deletionmap_data.len() / 8)
                    .map(|_| deletionmap_bytes.read_u64())
                    .collect();
                let bitset = ImmutableBitset::new(&words);
                return Self { doc_count, bitset };
            } else {
                warn!(
                    "Segment `{}` deletionmap data corrupted",
                    segment_id.as_str()
                );
            }
        }

        let bitset = ImmutableBitset::with_capacity(doc_count);
        Self { doc_count, bitset }
    }

    pub fn bitset(&self) -> &ImmutableBitset {
        &self.bitset
    }

    pub fn is_deleted(&self, docid: DocId) -> bool {
        self.bitset.contains(docid as usize)
    }

    pub fn deleted_doc_count(&self) -> usize {
        self.bitset.count_ones()
    }
}

impl MutableDeletionMap {
    pub fn with_doc_count(doc_count: usize) -> Self {
        Self {
            doc_count,
            bitset: MutableBitset::with_capacity(doc_count),
        }
    }

    pub fn copy_immutable_at(
        &mut self,
        immutable: &ImmutableDeletionMap,
        base_docid: DocId,
        doc_count: usize,
    ) {
        self.bitset
            .copy_data_at(immutable.bitset.data(), base_docid as usize, doc_count);
    }

    pub fn bitset(&self) -> &MutableBitset {
        &self.bitset
    }
}

impl From<ImmutableDeletionMap> for DeletionMap {
    fn from(immutable: ImmutableDeletionMap) -> Self {
        Self {
            doc_count: immutable.doc_count,
            bitset: immutable.bitset.into(),
        }
    }
}

impl DeletionMap {
    pub fn new(doc_count: usize, bitset: Bitset) -> Self {
        Self { doc_count, bitset }
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    pub fn is_deleted(&self, docid: DocId) -> bool {
        self.bitset.contains(docid as usize)
    }
}
