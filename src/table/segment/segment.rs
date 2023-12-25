use std::{path::Path, sync::Arc};

use crate::{column::ColumnSegmentData, index::IndexSegmentData, schema::SchemaRef};

use super::{SegmentColumnData, SegmentIndexData, SegmentMeta};

pub struct Segment {
    name: String,
    meta: SegmentMeta,
    index_data: SegmentIndexData,
    column_data: SegmentColumnData,
}

impl Segment {
    pub fn open(segment_name: String, schema: &SchemaRef, directory: impl AsRef<Path>) -> Self {
        let directory = directory.as_ref();
        let segment_directory = directory.join(&segment_name);
        let meta = SegmentMeta::load(segment_directory.join("meta.json"));

        let index_directory = segment_directory.join("index");
        let index_data = SegmentIndexData::open(index_directory, schema);

        let column_directory = segment_directory.join("column");
        let column_data = SegmentColumnData::open(column_directory, schema);

        Self {
            name: segment_name,
            meta,
            index_data,
            column_data,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
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
}
