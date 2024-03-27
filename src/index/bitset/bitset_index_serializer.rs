use std::io::Write;

use tantivy_common::TerminatingWrite;

use crate::{index::IndexSerializer, util::MutableBitset};

use super::BitsetIndexBuildingSegmentData;

#[derive(Default)]
pub struct BitsetIndexSerializer {}

impl IndexSerializer for BitsetIndexSerializer {
    fn serialize(
        &self,
        index: &crate::schema::IndexRef,
        index_data: &std::sync::Arc<dyn crate::index::IndexSegmentData>,
        directory: &dyn crate::Directory,
        index_path: &std::path::Path,
        doc_count: usize,
        docid_mapping: Option<&Vec<Option<crate::DocId>>>,
    ) {
        let bitset_index_data = index_data
            .clone()
            .downcast_arc::<BitsetIndexBuildingSegmentData>()
            .ok()
            .unwrap();
        let index_path = index_path.join(index.name());
        if let Some(docid_mapping) = docid_mapping {
            if !bitset_index_data.values.is_empty() {
                let mut remain_values = MutableBitset::with_capacity(doc_count);
                for docid in bitset_index_data.values.iter() {
                    if let Some(docid) = docid_mapping[docid] {
                        remain_values.insert(docid as usize);
                    }
                }
                let values_path = index_path.join("values");
                let mut values_writer = directory.open_write(&values_path).unwrap();
                for word in remain_values.data() {
                    values_writer.write_all(&word.to_le_bytes()).unwrap();
                }
                values_writer.terminate().unwrap();
            }
            if index.is_nullable() {
                if let Some(nulls) = bitset_index_data.nulls.as_ref() {
                    if !nulls.is_empty() {
                        let mut remain_nulls = MutableBitset::with_capacity(doc_count);
                        for docid in nulls.iter() {
                            if let Some(docid) = docid_mapping[docid] {
                                remain_nulls.insert(docid as usize);
                            }
                        }
                        let nulls_path = index_path.join("nulls");
                        let mut nulls_writer = directory.open_write(&nulls_path).unwrap();
                        for word in remain_nulls.data() {
                            nulls_writer.write_all(&word.to_le_bytes()).unwrap();
                        }
                        nulls_writer.terminate().unwrap();
                    }
                }
            }
        } else {
            if !bitset_index_data.values.is_empty() {
                let values_path = index_path.join("values");
                let mut values_writer = directory.open_write(&values_path).unwrap();
                for word in bitset_index_data.values.iter_words() {
                    values_writer.write_all(&word.to_le_bytes()).unwrap();
                }
                values_writer.terminate().unwrap();
            }
            if index.is_nullable() {
                if let Some(nulls) = bitset_index_data.nulls.as_ref() {
                    if !nulls.is_empty() {
                        let nulls_path = index_path.join("nulls");
                        let mut nulls_writer = directory.open_write(&nulls_path).unwrap();
                        for word in nulls.iter_words() {
                            nulls_writer.write_all(&word.to_le_bytes()).unwrap();
                        }
                        nulls_writer.terminate().unwrap();
                    }
                }
            }
        }
    }
}
