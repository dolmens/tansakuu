use crate::{
    util::{ExpandableBitset, ExpandableBitsetWriter},
    DocId,
};

pub struct BuildingDeletionMapWriter {
    bitset: ExpandableBitsetWriter,
}

#[derive(Clone)]
pub struct BuildingDeletionMap {
    bitset: ExpandableBitset,
}

impl BuildingDeletionMapWriter {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bitset: ExpandableBitsetWriter::with_capacity(capacity),
        }
    }

    pub fn delete_document(&mut self, docid: DocId) {
        self.bitset.insert(docid as usize);
    }

    pub fn deletionmap(&self) -> BuildingDeletionMap {
        BuildingDeletionMap {
            bitset: self.bitset.bitset(),
        }
    }
}

impl BuildingDeletionMap {
    pub fn is_deleted(&self, docid: DocId) -> bool {
        self.bitset.contains(docid as usize)
    }

    pub fn bitset(&self) -> &ExpandableBitset {
        &self.bitset
    }

    pub fn deleted_doc_count(&self) -> usize {
        self.bitset.count_ones()
    }
}
