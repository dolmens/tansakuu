use std::{path::PathBuf, sync::Arc};

use crate::{
    columnar::ColumnPersistentSegmentData, deletionmap::ImmutableDeletionMap,
    index::IndexSegmentData, schema::SchemaRef, Directory,
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
    deletionmap: ImmutableDeletionMap,
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
    pub fn open(directory: &dyn Directory, segment_id: SegmentId, schema: &SchemaRef) -> Self {
        let segment_path = PathBuf::from("segments").join(segment_id.as_str());
        let meta = SegmentMetaData::load(directory, segment_path.join("meta.json"));

        let index_path = segment_path.join("index");
        let index_data = PersistentSegmentIndexData::open(directory, index_path, schema);

        let column_data = PersistentSegmentColumnData::open(directory, &segment_path, schema);

        let deletionmap =
            ImmutableDeletionMap::load(directory, segment_id.clone(), meta.doc_count());

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

    pub fn column_data(&self, column: &str) -> Option<&ColumnPersistentSegmentData> {
        self.column_data.column(column)
    }

    pub fn deletionmap(&self) -> &ImmutableDeletionMap {
        &self.deletionmap
    }
}
