use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};

use crate::DocId;

use super::{ColumnMerger, ColumnPersistentSegmentData};

#[derive(Default)]
pub struct StringColumnMerger {}

impl ColumnMerger for StringColumnMerger {
    fn merge(
        &self,
        segments: &[&ColumnPersistentSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    ) -> ArrayRef {
        let mut values = Vec::<&str>::new();
        for (&segment, segment_docid_mapping) in segments.iter().zip(docid_mappings.iter()) {
            let data = segment
                .array()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for (i, docid) in segment_docid_mapping.iter().enumerate() {
                if docid.is_some() {
                    values.push(data.value(i));
                }
            }
        }

        Arc::new(StringArray::from(values))
    }
}
