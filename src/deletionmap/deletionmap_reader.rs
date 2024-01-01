use std::sync::Arc;

use crate::{
    table::{SegmentId, SegmentMetaInfo, TableData},
    DocId,
};

use super::{BuildingDeletionMap, DeletionMap};

pub struct DeletionMapReader {
    segments_meta_info: Vec<SegmentMetaInfo>,
    building_segments: Vec<Arc<BuildingDeletionMap>>,
    segments: Vec<Arc<DeletionMap>>,
}

impl DeletionMapReader {
    pub fn new(table_data: &TableData) -> Self {
        let mut segments_meta_info = vec![];
        let mut building_segments = vec![];
        let mut segments = vec![];

        for segment in table_data.segments() {
            segments_meta_info.push(segment.meta_info().clone());
            segment
                .segment()
                .deletionmap()
                .map(|deletionmap| segments.push(deletionmap.clone()));
        }

        for building_segment in table_data.building_segments() {
            segments_meta_info.push(building_segment.meta_info().clone());
            building_segments.push(building_segment.segment().deletemap().clone());
        }

        Self {
            segments_meta_info,
            building_segments,
            segments,
        }
    }

    pub fn is_deleted(&self, docid: DocId) -> bool {
        let (segment_id, docid) = self
            .segments_meta_info
            .iter()
            .find(|&meta_info| docid < meta_info.end_docid())
            .map(|meta_info| (meta_info.segment_id(), meta_info.inner_docid(docid)))
            .unwrap();
        self.is_deleted_in_segment(segment_id, docid)
    }

    pub(crate) fn is_deleted_in_segment(&self, segment_id: &SegmentId, docid: DocId) -> bool {
        for deletionmap in self.building_segments.iter().rev() {
            if deletionmap.is_deleted(segment_id.clone(), docid) {
                return true;
            }
        }
        for deletionmap in self.segments.iter().rev() {
            if deletionmap.is_deleted(segment_id, docid) {
                return true;
            }
        }
        false
    }
}
