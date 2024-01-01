use std::{path::Path, sync::Arc};

use crate::{
    column::ColumnSegmentData, deletionmap::DeletionMap, index::IndexSegmentData, schema::SchemaRef,
};

use super::{SegmentColumnData, SegmentId, SegmentIndexData, SegmentMeta};

pub struct Segment {
    segment_id: SegmentId,
    meta: SegmentMeta,
    index_data: SegmentIndexData,
    column_data: SegmentColumnData,
    deletionmap: Option<Arc<DeletionMap>>,
}

impl Segment {
    pub fn open(segment_id: SegmentId, schema: &SchemaRef, directory: impl AsRef<Path>) -> Self {
        let directory = directory.as_ref();
        let segment_directory = directory.join(segment_id.as_str());
        let meta = SegmentMeta::load(segment_directory.join("meta.json"));

        let index_directory = segment_directory.join("index");
        let index_data = SegmentIndexData::open(index_directory, schema);

        let column_directory = segment_directory.join("column");
        let column_data = SegmentColumnData::open(column_directory, schema);

        let deletionmap_path = segment_directory.join("deletionmap");
        let deletionmap = if deletionmap_path.exists() {
            Some(Arc::new(DeletionMap::load(deletionmap_path)))
        } else {
            None
        };

        Self {
            segment_id,
            meta,
            index_data,
            column_data,
            deletionmap,
        }
    }

    pub fn segment_id(&self) -> &SegmentId {
        &self.segment_id
    }

    pub fn doc_count(&self) -> usize {
        self.meta.doc_count()
    }

    pub fn index_data(&self, index: &str) -> &Arc<dyn IndexSegmentData> {
        self.index_data.index(index).unwrap()
    }

    pub fn column_data(&self, column: &str) -> &Arc<dyn ColumnSegmentData> {
        self.column_data.column(column).unwrap()
    }

    pub fn deletionmap(&self) -> Option<&Arc<DeletionMap>> {
        self.deletionmap.as_ref()
    }
}
