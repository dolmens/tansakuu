use std::sync::{atomic::AtomicBool, Arc};

use crate::deletionmap::BuildingDeletionMap;

use super::{
    BuildingDocCount, BuildingSegmentColumnData, BuildingSegmentIndexData, SegmentId, SegmentMeta,
    SegmentStat,
};

#[derive(Clone)]
pub struct BuildingSegment {
    meta: SegmentMeta,
    data: Arc<BuildingSegmentData>,
}

pub struct BuildingSegmentData {
    segment_id: SegmentId,
    doc_count: BuildingDocCount,
    dumping: AtomicBool,
    column_data: BuildingSegmentColumnData,
    index_data: BuildingSegmentIndexData,
    deletionmap: BuildingDeletionMap,
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

    pub fn is_dumping(&self) -> bool {
        self.data.is_dumping()
    }

    pub fn collect_segment_stat(&self) -> SegmentStat {
        let mut segment_stat = SegmentStat::new();
        segment_stat.doc_count = self.data.doc_count().get();
        self.data.collect_segment_stat(&mut segment_stat);
        segment_stat
    }
}

impl BuildingSegmentData {
    pub fn new(
        doc_count: BuildingDocCount,
        column_data: BuildingSegmentColumnData,
        index_data: BuildingSegmentIndexData,
        deletionmap: BuildingDeletionMap,
    ) -> Self {
        Self {
            segment_id: SegmentId::new(),
            doc_count,
            dumping: AtomicBool::new(false),
            column_data,
            index_data,
            deletionmap,
        }
    }

    pub fn segment_id(&self) -> &SegmentId {
        &self.segment_id
    }

    pub fn doc_count(&self) -> &BuildingDocCount {
        &self.doc_count
    }

    pub fn is_dumping(&self) -> bool {
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

    pub fn deletionmap(&self) -> &BuildingDeletionMap {
        &self.deletionmap
    }

    pub fn collect_segment_stat(&self, segment_stat: &mut SegmentStat) {
        self.index_data.collect_segment_stat(segment_stat);
    }
}
