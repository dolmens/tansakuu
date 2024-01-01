use std::sync::Arc;

use crate::{table::SegmentId, DocId};

use super::BuildingDeletionMap;

pub struct DeletionMapWriter {
    deletionmap: Arc<BuildingDeletionMap>,
}

impl DeletionMapWriter {
    pub fn new() -> Self {
        Self {
            deletionmap: Arc::new(BuildingDeletionMap::default()),
        }
    }

    pub fn deletionmap(&self) -> &Arc<BuildingDeletionMap> {
        &self.deletionmap
    }

    pub fn is_deleted(&self, segment_id: SegmentId, docid: DocId) -> bool {
        self.deletionmap.is_deleted(segment_id, docid)
    }

    pub fn delete_doc(&self, segment_id: SegmentId, docid: DocId) {
        self.deletionmap.delete_doc(segment_id, docid);
    }

    pub fn is_empty(&self) -> bool {
        self.deletionmap.is_empty()
    }
}
