use std::sync::Arc;

use crate::{
    table::{SegmentId, SegmentMeta, TableData},
    DocId,
};

use super::{BuildingDeletionMap, DeletionMap};

pub struct DeletionMapReader {
    segment_metas: Vec<SegmentMeta>,
    building_segments: Vec<Arc<BuildingDeletionMap>>,
    persistent_segments: Vec<Arc<DeletionMap>>,
}

impl DeletionMapReader {
    pub fn new(table_data: &TableData) -> Self {
        let mut segment_metas = vec![];
        let mut building_segments = vec![];
        let mut persistent_segments = vec![];

        for segment in table_data.persistent_segments() {
            segment_metas.push(segment.meta().clone());
            segment
                .data()
                .deletionmap()
                .map(|deletionmap| persistent_segments.push(deletionmap.clone()));
        }

        for segment in table_data.building_segments() {
            segment_metas.push(segment.meta().clone());
            building_segments.push(segment.data().deletemap().clone());
        }

        Self {
            segment_metas,
            building_segments,
            persistent_segments,
        }
    }

    pub fn is_deleted(&self, docid: DocId) -> bool {
        let (segment_id, docid) = self
            .segment_metas
            .iter()
            .find(|&meta| docid < meta.end_docid())
            .map(|meta| (meta.segment_id(), meta.inner_docid(docid)))
            .unwrap();
        self.is_deleted_in_segment(segment_id, docid)
    }

    pub(crate) fn is_deleted_in_segment(&self, segment_id: &SegmentId, docid: DocId) -> bool {
        for deletionmap in self.building_segments.iter().rev() {
            if deletionmap.is_deleted(segment_id, docid) {
                return true;
            }
        }
        for deletionmap in self.persistent_segments.iter().rev() {
            if deletionmap.is_deleted(segment_id, docid) {
                return true;
            }
        }
        false
    }
}
