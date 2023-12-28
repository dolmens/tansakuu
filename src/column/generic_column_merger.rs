use std::marker::PhantomData;

use crate::RowId;

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
        doc_counts: &[usize],
    ) {
        let path = directory.join(field.name());
        let mut writer = GenericColumnSerializerWriter::<T>::new(path);

        for (&segment, &doc_count) in segments.iter().zip(doc_counts.iter()) {
            let segment_data = segment
                .downcast_ref::<GenericColumnSegmentData<T>>()
                .unwrap();
            for i in 0..doc_count {
                writer.write(segment_data.get(i as RowId).unwrap());
            }
        }
    }
}
