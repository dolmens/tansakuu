use crate::schema::{Index, IndexType};

use super::{primary_key::PrimaryKeyIndexWriter, term::TermIndexWriter, IndexWriter};

#[derive(Default)]
pub struct IndexWriterFactory {}

impl IndexWriterFactory {
    pub fn create(&self, index: &Index) -> Box<dyn IndexWriter> {
        match index.index_type() {
            IndexType::Term => Box::new(TermIndexWriter::new()),
            IndexType::PrimaryKey => Box::new(PrimaryKeyIndexWriter::new()),
        }
    }
}
