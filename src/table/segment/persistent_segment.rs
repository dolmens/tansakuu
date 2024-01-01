use std::{path::Path, sync::Arc};

use crate::{
    column::ColumnSegmentData, deletionmap::DeletionMap, index::IndexSegmentData, schema::SchemaRef,
};

use super::{
    PersistentSegmentColumnData, PersistentSegmentIndexData, SegmentId, SegmentMeta,
    SegmentMetaData,
};

#[derive(Clone)]
pub struct PersistentSegment {
    meta: SegmentMeta,
    data: Arc<PersistentSegmentData>,
}

pub struct PersistentSegmentData {
    segment_id: SegmentId,
    meta: SegmentMetaData,
    index_data: PersistentSegmentIndexData,
    column_data: PersistentSegmentColumnData,
    deletionmap: Option<Arc<DeletionMap>>,
}

impl PersistentSegment {
    pub fn new(meta: SegmentMeta, data: Arc<PersistentSegmentData>) -> Self {
        Self { meta, data }
    }

    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    pub fn data(&self) -> &Arc<PersistentSegmentData> {
        &self.data
    }
}

impl PersistentSegmentData {
    pub fn open(segment_id: SegmentId, schema: &SchemaRef, directory: impl AsRef<Path>) -> Self {
        let directory = directory.as_ref();
        let segment_directory = directory.join(segment_id.as_str());
        let meta = SegmentMetaData::load(segment_directory.join("meta.json"));

        let index_directory = segment_directory.join("index");
        let index_data = PersistentSegmentIndexData::open(index_directory, schema);

        let column_directory = segment_directory.join("column");
        let column_data = PersistentSegmentColumnData::open(column_directory, schema);

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
