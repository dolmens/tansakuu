use crate::{index::IndexSegmentData, util::layered_hashmap::LayeredHashMap, DocId};

pub struct UniqueKeyBuildingSegmentData {
    pub keys: LayeredHashMap<u64, DocId>,
}

impl UniqueKeyBuildingSegmentData {
    pub fn new(keys: LayeredHashMap<u64, DocId>) -> Self {
        Self { keys }
    }
}

impl IndexSegmentData for UniqueKeyBuildingSegmentData {}
