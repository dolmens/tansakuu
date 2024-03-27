use std::sync::Arc;

use arrow::{
    array::BooleanArray,
    buffer::{BooleanBuffer, NullBuffer},
};

use super::{BooleanColumnBuildingSegmentData, ColumnSerializer};

#[derive(Default)]
pub struct BooleanColumnSerializer {}

impl ColumnSerializer for BooleanColumnSerializer {
    fn serialize(
        &self,
        column_data: &dyn super::ColumnBuildingSegmentData,
        doc_count: usize,
        _docid_mapping: Option<&Vec<Option<crate::DocId>>>,
    ) -> arrow::array::ArrayRef {
        let boolean_column_data = column_data
            .as_any()
            .downcast_ref::<BooleanColumnBuildingSegmentData>()
            .unwrap();

        let values: Vec<_> = boolean_column_data
            .values
            .iter_values()
            .take(doc_count)
            .collect();
        if boolean_column_data.nullable {
            let mut nulls = vec![true; values.len()];
            for (i, is_null) in boolean_column_data
                .nulls
                .as_ref()
                .unwrap()
                .iter_values()
                .take(values.len())
                .enumerate()
            {
                if is_null {
                    nulls[i] = false;
                }
            }
            let values_buffer: BooleanBuffer = values.into();
            let nulls_buffer: BooleanBuffer = nulls.into();
            let array = BooleanArray::new(values_buffer, Some(NullBuffer::new(nulls_buffer)));
            Arc::new(array)
        } else {
            let array: BooleanArray = values.into();
            Arc::new(array)
        }
    }
}
