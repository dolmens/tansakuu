use std::path::PathBuf;

use crate::{
    deletionmap::DeletionMapReader, index::IndexMergerFactory, schema::SchemaRef, table::Version,
    Directory, DocId,
};

use super::{PersistentSegmentData, SegmentColumnMerger, SegmentId, SegmentMetaData};

#[derive(Default)]
pub struct SegmentMerger {}

impl SegmentMerger {
    pub fn merge(
        &self,
        directory: &dyn Directory,
        schema: &SchemaRef,
        deletionmap_reader: &DeletionMapReader,
        version: &mut Version,
    ) {
        let segment_directory = PathBuf::from("segments");
        let segments: Vec<_> = version
            .segments()
            .iter()
            .map(|segment_id| PersistentSegmentData::open(directory, segment_id.clone(), schema))
            .collect();

        let segment_id = SegmentId::new();
        let segment_path = segment_directory.join(segment_id.as_str());

        let mut docid = 0;
        let mut docid_mappings = Vec::<Vec<Option<DocId>>>::new();
        let mut remain_segments = vec![];
        for segment in segments.iter() {
            let mut segment_docid_mapping = vec![];
            let deletionmap_segment_reader =
                deletionmap_reader.segment_reader(segment.segment_id());
            for i in 0..segment.doc_count() {
                segment_docid_mapping.push(
                    if deletionmap_segment_reader
                        .is_some_and(|reader| reader.is_deleted(i as DocId))
                    {
                        None
                    } else {
                        let current_docid = docid;
                        docid += 1;
                        Some(current_docid)
                    },
                );
            }
            let deleted_doc_count = segment_docid_mapping.iter().filter(|m| m.is_none()).count();
            if deleted_doc_count < segment.doc_count() {
                docid_mappings.push(segment_docid_mapping);
                remain_segments.push(segment);
            }
        }

        let total_doc_count = docid as usize;
        if !remain_segments.is_empty() {
            let column_merger = SegmentColumnMerger::default();
            column_merger.merge(
                directory,
                &segment_path,
                schema,
                &remain_segments,
                &docid_mappings,
            );

            let index_path = segment_path.join("index");
            let index_merger_factory = IndexMergerFactory::default();
            for index in schema.indexes() {
                let index_merger = index_merger_factory.create(index);
                let index_data: Vec<_> = remain_segments
                    .iter()
                    .map(|seg| seg.index_data(index.name()).unwrap())
                    .collect();
                index_merger.merge(
                    directory,
                    &index_path,
                    index,
                    total_doc_count,
                    &index_data,
                    &docid_mappings,
                );
            }

            let meta = SegmentMetaData::new(total_doc_count);
            meta.save(directory, segment_path.join("meta.json"));
            version.add_segment(segment_id);
        }

        for segment in segments.iter() {
            version.remove_segment(segment.segment_id());
        }
        version.save(directory);
    }
}
