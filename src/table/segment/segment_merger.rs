use std::{fs, path::Path};

use uuid::Uuid;

use crate::{
    column::ColumnMergerFactory, index::IndexMergerFactory, schema::SchemaRef, table::Version,
};

use super::{Segment, SegmentMeta};

#[derive(Default)]
pub struct SegmentMerger {}

impl SegmentMerger {
    pub fn merge(&self, directory: impl AsRef<Path>, schema: &SchemaRef, version: &mut Version) {
        let directory = directory.as_ref();
        let segment_directory = directory.join("segments");
        let segments: Vec<_> = version
            .segments()
            .iter()
            .map(|segment_name| Segment::new(segment_name.clone(), schema, &segment_directory))
            .collect();

        let segment_uuid = Uuid::new_v4();
        let segment_uuid_string = segment_uuid.as_simple().to_string();
        let segment_directory = segment_directory.join(&segment_uuid_string);

        let doc_counts: Vec<_> = segments.iter().map(|seg| seg.doc_count()).collect();

        let column_directory = segment_directory.join("column");
        fs::create_dir_all(&column_directory).unwrap();
        let column_merger_factory = ColumnMergerFactory::default();
        for field in schema.columns() {
            let column_merger = column_merger_factory.create(field);
            let column_data: Vec<_> = segments
                .iter()
                .map(|seg| seg.column_data(field.name()).as_ref())
                .collect();
            column_merger.merge(&column_directory, field, &column_data, &doc_counts);
        }

        let index_directory = segment_directory.join("index");
        fs::create_dir_all(&index_directory).unwrap();
        let index_merger_factory = IndexMergerFactory::default();
        for index in schema.indexes() {
            let index_merger = index_merger_factory.create(index);
            let index_data: Vec<_> = segments
                .iter()
                .map(|seg| seg.index_data(index.name()).as_ref())
                .collect();
            index_merger.merge(&index_directory, index, &index_data, &doc_counts);
        }

        let meta = SegmentMeta::new(doc_counts.iter().sum());
        meta.save(segment_directory.join("meta.json"));

        for segment in segments.iter() {
            version.remove_segment(segment.name());
        }
        version.add_segment(segment_uuid_string);
        version.save(directory);
    }
}
