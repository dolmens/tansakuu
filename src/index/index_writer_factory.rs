use crate::schema::{Index, IndexType};

use super::{term::TermIndexWriter, unique_key::UniqueKeyIndexWriter, IndexWriter};

pub struct IndexWriterFactory {}

impl IndexWriterFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create(&self, index: &Index) -> Box<dyn IndexWriter> {
        match index.index_type() {
            IndexType::Term => Box::new(TermIndexWriter::new()),
            IndexType::UniqueKey => Box::new(UniqueKeyIndexWriter::new()),
        }
    }
}
