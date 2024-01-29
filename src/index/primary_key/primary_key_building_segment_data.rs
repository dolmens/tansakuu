use crate::{index::IndexSegmentData, util::layered_hashmap::LayeredHashMap, DocId};

pub struct PrimaryKeyBuildingSegmentData {
    pub keys: LayeredHashMap<String, DocId>,
}

impl PrimaryKeyBuildingSegmentData {
    pub fn new(keys: LayeredHashMap<String, DocId>) -> Self {
        Self { keys }
    }
}

impl IndexSegmentData for PrimaryKeyBuildingSegmentData {}
