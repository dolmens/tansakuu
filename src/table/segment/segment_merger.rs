use std::path::PathBuf;

use crate::{
    column::ColumnMergerFactory, index::IndexMergerFactory, schema::SchemaRef, table::Version,
    Directory, DocId,
};

use super::{PersistentSegmentData, SegmentId, SegmentMetaData};

#[derive(Default)]
pub struct SegmentMerger {}

impl SegmentMerger {
    pub fn merge(&self, directory: &dyn Directory, schema: &SchemaRef, version: &mut Version) {
        let segment_directory = PathBuf::from("segments");
        let segments: Vec<_> = version
            .segments()
            .iter()
            .map(|segment_id| PersistentSegmentData::open(directory, segment_id.clone(), schema))
            .collect();

        let segment_id = SegmentId::new();
        let segment_directory = segment_directory.join(segment_id.as_str());

        let mut docid = 0;
        let mut docid_mappings = Vec::<Vec<Option<DocId>>>::new();
        for segment in segments.iter() {
            let mut segment_docid_mappings = vec![];
            let deletionmap = segment.deletionmap();
            for i in 0..segment.doc_count() {
                if deletionmap.is_deleted(i as DocId) {
                    segment_docid_mappings.push(None);
                } else {
                    segment_docid_mappings.push(Some(docid));
                    docid += 1;
                }
            }
            docid_mappings.push(segment_docid_mappings);
        }

        let doc_count = docid_mappings
            .iter()
            .flatten()
            .filter(|&docid| docid.is_some())
            .count();

        // TODO: if doc_count is 0

        let column_directory = segment_directory.join("column");
        let column_merger_factory = ColumnMergerFactory::default();
        for field in schema.columns() {
            let column_merger = column_merger_factory.create(field);
            let column_data: Vec<_> = segments
                .iter()
                .map(|seg| seg.column_data(field.name()).as_ref())
                .collect();
            column_merger.merge(
                directory,
                &column_directory,
                field,
                &column_data,
                &docid_mappings,
            );
        }

        let index_directory = segment_directory.join("index");
        let index_merger_factory = IndexMergerFactory::default();
        for index in schema.indexes() {
            let index_merger = index_merger_factory.create(index);
            let index_data: Vec<_> = segments
                .iter()
                .map(|seg| seg.index_data(index.name()))
                .collect();
            index_merger.merge(
                directory,
                &index_directory,
                index,
                &index_data,
                &docid_mappings,
            );
        }

        let meta = SegmentMetaData::new(doc_count);
        meta.save(directory, segment_directory.join("meta.json"));

        for segment in segments.iter() {
            version.remove_segment(segment.segment_id());
        }
        version.add_segment(segment_id);
        version.save(directory);
    }
}
