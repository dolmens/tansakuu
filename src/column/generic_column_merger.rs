use std::marker::PhantomData;

use crate::DocId;

use super::{ColumnMerger, GenericColumnSegmentData, GenericColumnSerializerWriter};

#[derive(Default)]
pub struct GenericColumnMerger<T> {
    _marker: PhantomData<T>,
}

impl<T: ToString + Clone + Send + Sync + 'static> ColumnMerger for GenericColumnMerger<T> {
    fn merge(
        &self,
        directory: &std::path::Path,
        field: &crate::schema::Field,
        segments: &[&dyn super::ColumnSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        let path = directory.join(field.name());
        let mut writer = GenericColumnSerializerWriter::<T>::new(path);

        for (&segment, segment_docid_mappings) in segments.iter().zip(docid_mappings.iter()) {
            let segment_data = segment
                .downcast_ref::<GenericColumnSegmentData<T>>()
                .unwrap();
            for docid in segment_docid_mappings {
                if let Some(docid) = docid.clone() {
                    writer.write(segment_data.get(docid).unwrap());
                }
            }
        }
    }
}
