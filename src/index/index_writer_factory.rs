use crate::schema::{Index, IndexType};

use super::{term::TermIndexWriter, unique_key::UniqueKeyIndexWriter, IndexWriter};

#[derive(Default)]
pub struct IndexWriterFactory {}

impl IndexWriterFactory {
    pub fn create(&self, index: &Index) -> Box<dyn IndexWriter> {
        match index.index_type() {
            IndexType::Term => Box::new(TermIndexWriter::new()),
            IndexType::UniqueKey => Box::new(UniqueKeyIndexWriter::new()),
        }
    }
}
