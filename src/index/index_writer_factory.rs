use std::sync::Arc;

use crate::{
    schema::{IndexRef, IndexType},
    table::SegmentStat,
};

use super::{inverted_index::InvertedIndexWriter, unique_key::UniqueKeyWriter, IndexWriter};

#[derive(Default)]
pub struct IndexWriterFactory {}

impl IndexWriterFactory {
    pub fn create(
        &self,
        index: &IndexRef,
        recent_segment_stat: Option<&Arc<SegmentStat>>,
    ) -> Box<dyn IndexWriter> {
        match index.index_type() {
            IndexType::Text(_) => Box::new(InvertedIndexWriter::new(
                index.clone(),
                recent_segment_stat.clone(),
            )),
            IndexType::UniqueKey => Box::new(UniqueKeyWriter::new(recent_segment_stat.clone())),
        }
    }
}
