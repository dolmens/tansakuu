use std::collections::HashMap;

use crate::index::IndexSegmentReader;

pub struct BuildingSegmentIndexReader {
    indexes: HashMap<String, Box<dyn IndexSegmentReader>>,
}

impl BuildingSegmentIndexReader {
    pub fn index(&self, name: &str) -> Option<&dyn IndexSegmentReader> {
        self.indexes.get(name).map(|index| index.as_ref())
    }
}
