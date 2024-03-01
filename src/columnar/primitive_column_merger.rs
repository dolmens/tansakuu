use std::{marker::PhantomData, sync::Arc};

use arrow::array::{ArrayRef, PrimitiveArray};

use crate::{types::PrimitiveType, DocId};

use super::{ColumnMerger, ColumnPersistentSegmentData};

pub struct PrimitiveColumnMerger<T: PrimitiveType> {
    _marker: PhantomData<T>,
}

impl<T: PrimitiveType> Default for PrimitiveColumnMerger<T> {
    fn default() -> Self {
        PrimitiveColumnMerger {
            _marker: PhantomData,
        }
    }
}

impl<T: PrimitiveType> ColumnMerger for PrimitiveColumnMerger<T>
where
    PrimitiveArray<T::ArrowPrimitive>: From<Vec<T::Native>>,
{
    fn merge(
        &self,
        segments: &[&ColumnPersistentSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    ) -> ArrayRef {
        let mut values = Vec::<T::Native>::new();
        for (&segment, segment_docid_mapping) in segments.iter().zip(docid_mappings.iter()) {
            let data = segment
                .array()
                .as_any()
                .downcast_ref::<PrimitiveArray<T::ArrowPrimitive>>()
                .unwrap();
            for (i, docid) in segment_docid_mapping.iter().enumerate() {
                if docid.is_some() {
                    values.push(data.value(i));
                }
            }
        }

        Arc::new(PrimitiveArray::<T::ArrowPrimitive>::from(values))
    }
}
