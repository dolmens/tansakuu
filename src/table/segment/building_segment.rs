use crate::util::AcqRelUsize;

use super::{BuildingSegmentColumnData, BuildingSegmentIndexData};

pub struct BuildingSegment {
    doc_count: AcqRelUsize,
    column_data: BuildingSegmentColumnData,
    index_data: BuildingSegmentIndexData,
}

impl BuildingSegment {
    pub fn new(
        column_data: BuildingSegmentColumnData,
        index_data: BuildingSegmentIndexData,
    ) -> Self {
        Self {
            doc_count: AcqRelUsize::new(0),
            column_data,
            index_data,
        }
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count.load()
    }

    pub(super) fn set_doc_count(&self, doc_count: usize) {
        self.doc_count.store(doc_count);
    }

    pub fn column_data(&self) -> &BuildingSegmentColumnData {
        &self.column_data
    }

    pub fn index_data(&self) -> &BuildingSegmentIndexData {
        &self.index_data
    }
}
