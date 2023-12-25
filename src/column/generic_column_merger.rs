use std::{fs::File, io::Write, marker::PhantomData};

use super::{ColumnMerger, GenericColumnSegmentData};

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
        let mut file = File::create(path).unwrap();

        for (&segment, &doc_count) in segments.iter().zip(doc_counts.iter()) {
            let segment_data = segment
                .downcast_ref::<GenericColumnSegmentData<T>>()
                .unwrap();
            for i in 0..doc_count {
                writeln!(file, "{}", segment_data.get(i).unwrap().to_string()).unwrap();
            }
        }
    }
}
