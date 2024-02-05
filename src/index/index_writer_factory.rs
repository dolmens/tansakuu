use crate::schema::{Index, IndexType};

use super::{inverted_index::InvertedIndexWriter, primary_key::PrimaryKeyWriter, IndexWriter};

#[derive(Default)]
pub struct IndexWriterFactory {}

impl IndexWriterFactory {
    pub fn create(&self, index: &Index) -> Box<dyn IndexWriter> {
        match index.index_type() {
            IndexType::Text(_) => Box::new(InvertedIndexWriter::new(index.clone())),
            IndexType::PrimaryKey => Box::new(PrimaryKeyWriter::new()),
        }
    }
}
