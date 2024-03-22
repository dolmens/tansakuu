use arrow::array::ArrayRef;

use crate::DocId;

use super::ColumnBuildingSegmentData;

pub trait ColumnSerializer {
    fn serialize(
        &self,
        column_data: &dyn ColumnBuildingSegmentData,
        doc_count: usize,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    ) -> ArrayRef;
}
