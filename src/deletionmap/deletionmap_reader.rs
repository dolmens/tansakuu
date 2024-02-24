use crate::{table::TableData, DocId};

use super::{BuildingDeletionMap, DeletionMap};

pub struct DeletionMapReader {
    fixed_doc_count: usize,
    deletionmap: DeletionMap,
    building_deletionmap: Option<BuildingDeletionMap>,
}

impl DeletionMapReader {
    pub fn new(table_data: &TableData) -> Self {
        let fixed_doc_count = table_data.fixed_doc_count();
        let deletionmap = table_data.deletionmap().clone();
        let building_deletionmap = table_data
            .active_building_segment()
            .map(|seg| seg.data().deletionmap().clone());

        Self {
            fixed_doc_count,
            deletionmap,
            building_deletionmap,
        }
    }

    pub fn is_deleted(&self, docid: DocId) -> bool {
        if docid < self.fixed_doc_count as DocId {
            self.deletionmap.is_deleted(docid)
        } else if let Some(deletionmap) = self.building_deletionmap.as_ref() {
            deletionmap.is_deleted(docid - (self.fixed_doc_count as DocId))
        } else {
            false
        }
    }
}
