use std::sync::Arc;

use crate::{util::Bitset, DocId};

use super::BooleanColumnBuildingSegmentData;

pub struct BooleanColumnBuildingSegmentReader {
    nullable: bool,
    values: Bitset,
    nulls: Option<Bitset>,
}

impl BooleanColumnBuildingSegmentReader {
    pub fn new(column_data: Arc<BooleanColumnBuildingSegmentData>) -> Self {
        Self {
            nullable: column_data.nullable,
            values: column_data.values.clone(),
            nulls: column_data.nulls.clone(),
        }
    }

    pub fn get(&self, docid: DocId) -> Option<bool> {
        if self.nullable {
            // Note nulls size may be smaller than values
            if self.nulls.as_ref().unwrap().contains(docid as usize) {
                return None;
            }
        }
        Some(self.values.contains(docid as usize))
    }

    pub fn doc_count(&self) -> usize {
        unimplemented!()
        // self.values.len()
    }
}
