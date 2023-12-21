use std::sync::Arc;

use super::BuildingSegment;

pub struct SegmentDumper {}

impl SegmentDumper {
    pub fn new() -> Self {
        Self {}
    }

    pub fn dump_segment(&self, segment: Arc<BuildingSegment>) {

    }
}

impl Drop for SegmentDumper {
    fn drop(&mut self) {}
}
