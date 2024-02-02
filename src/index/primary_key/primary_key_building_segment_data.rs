use crate::{index::IndexSegmentData, util::layered_hashmap::LayeredHashMap, DocId};

pub struct PrimaryKeyBuildingSegmentData {
    pub keys: LayeredHashMap<u64, DocId>,
}

impl PrimaryKeyBuildingSegmentData {
    pub fn new(keys: LayeredHashMap<u64, DocId>) -> Self {
        Self { keys }
    }
}

impl IndexSegmentData for PrimaryKeyBuildingSegmentData {}
