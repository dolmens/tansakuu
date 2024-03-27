use std::io::Write;

use tantivy_common::TerminatingWrite;

use crate::{index::IndexMerger, util::MutableBitset};

use super::BitsetIndexPersistentSegmentData;

#[derive(Default)]
pub struct BitsetIndexMerger {}

impl IndexMerger for BitsetIndexMerger {
    fn merge(
        &self,
        directory: &dyn crate::Directory,
        index_path: &std::path::Path,
        index: &crate::schema::Index,
        total_doc_count: usize,
        segments: &[&std::sync::Arc<dyn crate::index::IndexSegmentData>],
        docid_mappings: &[Vec<Option<crate::DocId>>],
    ) {
        let mut values = MutableBitset::with_capacity(total_doc_count);
        let mut nulls = if index.is_nullable() {
            Some(MutableBitset::with_capacity(total_doc_count))
        } else {
            None
        };

        for (&segment, docid_mapping) in segments.iter().zip(docid_mappings.iter()) {
            let segment_data = segment
                .downcast_ref::<BitsetIndexPersistentSegmentData>()
                .unwrap();
            if let Some(segment_values) = segment_data.values.as_ref() {
                for docid in segment_values.iter() {
                    if let Some(docid) = docid_mapping[docid] {
                        values.insert(docid as usize);
                    } else {
                        println!("dddd: {}", docid);
                    }
                }
            }
            if index.is_nullable() {
                if let Some(segment_nulls) = segment_data.nulls.as_ref() {
                    for docid in segment_nulls.iter() {
                        if let Some(docid) = docid_mapping[docid] {
                            nulls.as_mut().unwrap().insert(docid as usize);
                        }
                    }
                }
            }
        }

        let index_path = index_path.join(index.name());
        if values.count_ones() > 0 {
            let values_path = index_path.join("values");
            let mut values_writer = directory.open_write(&values_path).unwrap();
            for word in values.data() {
                values_writer.write_all(&word.to_le_bytes()).unwrap();
            }
            values_writer.terminate().unwrap();
        }
        if index.is_nullable() {
            if nulls.as_ref().unwrap().count_ones() > 0 {
                let nulls_path = index_path.join("nulls");
                let mut nulls_writer = directory.open_write(&nulls_path).unwrap();
                for word in nulls.as_ref().unwrap().data() {
                    nulls_writer.write_all(&word.to_le_bytes()).unwrap();
                }
                nulls_writer.terminate().unwrap();
            }
        }
    }
}
