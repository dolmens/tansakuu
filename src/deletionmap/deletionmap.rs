use tantivy_common::HasLen;

use crate::{
    table::SegmentId,
    util::{Bitset, ImmutableBitset, MutableBitset},
    Directory, DocId,
};

use std::path::PathBuf;

#[derive(Clone)]
pub struct ImmutableDeletionMap {
    bitset: ImmutableBitset,
}

#[derive(Clone)]
pub struct MutableDeletionMap {
    bitset: MutableBitset,
}

impl Into<ImmutableDeletionMap> for MutableDeletionMap {
    fn into(self) -> ImmutableDeletionMap {
        ImmutableDeletionMap {
            bitset: self.bitset.into(),
        }
    }
}

#[derive(Clone)]
pub struct DeletionMap {
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
                return Self { bitset };
            } else {
                warn!(
                    "Segment `{}` deletionmap data corrupted",
                    segment_id.as_str()
                );
            }
        }

        let bitset = ImmutableBitset::with_capacity(doc_count);
        Self { bitset }
    }

    pub fn bitset(&self) -> &ImmutableBitset {
        &self.bitset
    }

    pub fn is_deleted(&self, docid: DocId) -> bool {
        self.bitset.contains(docid as usize)
    }
}

impl MutableDeletionMap {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bitset: MutableBitset::with_capacity(capacity),
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
            bitset: immutable.bitset.into(),
        }
    }
}

impl DeletionMap {
    pub fn new(bitset: Bitset) -> Self {
        Self { bitset }
    }

    pub fn is_deleted(&self, docid: DocId) -> bool {
        self.bitset.contains(docid as usize)
    }
}
