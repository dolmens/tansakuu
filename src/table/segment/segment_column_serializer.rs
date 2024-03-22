use std::{collections::HashMap, path::Path, sync::Arc};

use arrow::{
    array::{BooleanArray, RecordBatch},
    compute::filter,
    ipc::writer::FileWriter,
};
use tantivy_common::TerminatingWrite;

use crate::{
    columnar::{ColumnBuildingSegmentData, ColumnSerializerFactory},
    schema::{Schema, SchemaConverter},
    Directory, DocId,
};

pub struct SegmentColumnSerializer {}

impl SegmentColumnSerializer {
    pub fn serialize(
        &self,
        directory: &dyn Directory,
        segment_path: &Path,
        doc_count: usize,
        docid_mapping: Option<&Vec<Option<DocId>>>,
        schema: &Schema,
        column_data: &HashMap<String, Arc<dyn ColumnBuildingSegmentData>>,
    ) {
        if schema.columns().is_empty() {
            return;
        }

        let schema_converter = SchemaConverter::default();
        let arrow_schema = schema_converter.convert_to_arrow(schema);
        let arrow_schema = Arc::new(arrow_schema);

        let mut arrow_columns = vec![];
        let serializer_factory = ColumnSerializerFactory::default();
        for field in schema.columns() {
            let column_serializer = serializer_factory.create(field);
            let field_column_data = column_data.get(field.name()).unwrap().as_ref();
            let arrow_column =
                column_serializer.serialize(field_column_data, doc_count, docid_mapping);
            arrow_columns.push(arrow_column);
        }
        let arrow_columns: Vec<_> = if let Some(docid_mapping) = docid_mapping {
            let filter_array = docid_mapping
                .iter()
                .map(|d| d.map(|_| true))
                .collect::<BooleanArray>();
            arrow_columns
                .iter()
                .map(|c| filter(c, &filter_array).unwrap())
                .collect()
        } else {
            arrow_columns
        };

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
