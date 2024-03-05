use std::{collections::HashMap, io::Cursor, path::Path};

use arrow::ipc::reader::FileReader;

use crate::{
    columnar::ColumnPersistentSegmentData,
    schema::{ArrowSchemaValidator, Schema},
    Directory,
};

pub struct PersistentSegmentColumnData {
    columns: HashMap<String, ColumnPersistentSegmentData>,
}

impl PersistentSegmentColumnData {
    pub fn open(directory: &dyn Directory, segment_path: &Path, schema: &Schema) -> Self {
        let path = segment_path.join("columnar.arrow");
        let input = directory.open_read(&path).unwrap();
        let input_bytes = input.read_bytes().unwrap();
        let input_reader = Cursor::new(input_bytes.as_slice());
        let mut reader = FileReader::try_new(input_reader, None).unwrap();
        let record_batch = reader.next().unwrap().unwrap();

        let schema_validator = ArrowSchemaValidator::default();
        schema_validator
            .validate(schema, record_batch.schema().as_ref())
            .unwrap();

        let mut columns = HashMap::new();
        for (i, field) in schema.columns().iter().enumerate() {
            let values = record_batch.column(i).clone();
            let data = ColumnPersistentSegmentData::new(values);
            columns.insert(field.name().to_string(), data);
        }

        Self { columns }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnPersistentSegmentData> {
        self.columns.get(name)
    }
}
