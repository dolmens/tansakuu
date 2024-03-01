use std::{path::Path, sync::Arc};

use arrow::{
    array::BooleanArray,
    compute::{concat, filter},
    ipc::writer::FileWriter,
    record_batch::RecordBatch,
};
use tantivy_common::TerminatingWrite;

use crate::{
    schema::{Schema, SchemaConverter},
    Directory, DocId,
};

use super::PersistentSegmentData;

#[derive(Default)]
pub struct SegmentColumnMerger {}

impl SegmentColumnMerger {
    pub fn merge(
        &self,
        directory: &dyn Directory,
        segment_path: &Path,
        schema: &Schema,
        segments: &[&PersistentSegmentData],
        docid_mappings: &[Vec<Option<DocId>>],
    ) {
        let schema_converter = SchemaConverter::default();
        let arrow_schema = schema_converter.convert_to_arrow(schema);
        let arrow_schema = Arc::new(arrow_schema);

        let mut arrow_columns = vec![];
        for field in schema.columns() {
            let segment_arrays: Vec<_> = segments
                .iter()
                .map(|seg| seg.column_data(field.name()).unwrap().array().as_ref())
                .collect();
            let merged_array = concat(&segment_arrays).unwrap();
            arrow_columns.push(merged_array);
        }

        let docid_mappings: Vec<_> = docid_mappings
            .into_iter()
            .flat_map(|m| m)
            .cloned()
            .collect();
        let filter_array = docid_mappings
            .iter()
            .map(|d| d.map(|_| true))
            .collect::<BooleanArray>();
        let arrow_columns: Vec<_> = arrow_columns
            .iter()
            .map(|c| filter(c, &filter_array).unwrap())
            .collect();

        let record_batch = RecordBatch::try_new(arrow_schema.clone(), arrow_columns).unwrap();
        let output_path = segment_path.join("columnar.arrow");
        let mut output_writer = directory.open_write(&output_path).unwrap();
        let mut arrow_writer = FileWriter::try_new(&mut output_writer, &arrow_schema).unwrap();
        arrow_writer.write(&record_batch).unwrap();
        arrow_writer.finish().unwrap();
        drop(arrow_writer);
        output_writer.terminate().unwrap();
    }
}
