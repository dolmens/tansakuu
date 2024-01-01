use std::sync::{atomic::AtomicBool, Arc};

use crate::{deletionmap::BuildingDeletionMap, util::AcqRelUsize};

use super::{BuildingSegmentColumnData, BuildingSegmentIndexData, SegmentId, SegmentMeta};

#[derive(Clone)]
pub struct BuildingSegment {
    meta: SegmentMeta,
    data: Arc<BuildingSegmentData>,
}

pub struct BuildingSegmentData {
    segment_id: SegmentId,
    doc_count: AcqRelUsize,
    dumping: AtomicBool,
    column_data: BuildingSegmentColumnData,
    index_data: BuildingSegmentIndexData,
    deletemap: Arc<BuildingDeletionMap>,
}

impl BuildingSegment {
    pub fn new(meta: SegmentMeta, data: Arc<BuildingSegmentData>) -> Self {
        Self { meta, data }
    }

    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    pub fn meta_mut(&mut self) -> &mut SegmentMeta {
        &mut self.meta
    }

    pub fn data(&self) -> &Arc<BuildingSegmentData> {
        &self.data
    }
}

impl BuildingSegmentData {
    pub fn new(
        column_data: BuildingSegmentColumnData,
        index_data: BuildingSegmentIndexData,
        deletemap: Arc<BuildingDeletionMap>,
    ) -> Self {
        Self {
            segment_id: SegmentId::new(),
            doc_count: AcqRelUsize::new(0),
            dumping: AtomicBool::new(false),
            column_data,
            index_data,
            deletemap,
        }
    }

    pub fn segment_id(&self) -> &SegmentId {
        &self.segment_id
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count.load()
    }

    pub(super) fn set_doc_count(&self, doc_count: usize) {
        self.doc_count.store(doc_count);
    }

    pub fn dumping(&self) -> bool {
        self.dumping.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn set_dumping_start(&self) {
        self.dumping
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn column_data(&self) -> &BuildingSegmentColumnData {
        &self.column_data
    }

    pub fn index_data(&self) -> &BuildingSegmentIndexData {
        &self.index_data
    }

    pub fn deletemap(&self) -> &Arc<BuildingDeletionMap> {
        &self.deletemap
    }
}
