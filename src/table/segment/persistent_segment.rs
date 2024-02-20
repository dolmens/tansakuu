use std::{path::PathBuf, sync::Arc};

use crate::{
    column::ColumnSegmentData, deletionmap::DeletionMap, index::IndexSegmentData,
    schema::SchemaRef, Directory,
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
    pub fn open(directory: &dyn Directory, segment_id: SegmentId, schema: &SchemaRef) -> Self {
        let mut segment_directory = PathBuf::from("segments");
        segment_directory.push(segment_id.as_str());

        let meta = SegmentMetaData::load(directory, segment_directory.join("meta.json"));

        let index_directory = segment_directory.join("index");
        let index_data = PersistentSegmentIndexData::open(directory, index_directory, schema);

        let column_directory = segment_directory.join("column");
        let column_data = PersistentSegmentColumnData::open(directory, column_directory, schema);

        let deletionmap_path = segment_directory.join("deletionmap");
        let deletionmap = if directory.exists(&deletionmap_path).unwrap() {
            Some(Arc::new(DeletionMap::load(directory, deletionmap_path)))
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
