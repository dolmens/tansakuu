use std::sync::Arc;

use crate::{
    columnar::{ColumnReader, GenericColumnReader},
    DocId,
};

pub struct PrimaryKeyReader(Arc<dyn ColumnReader>);

pub struct TypedPrimaryKeyReader<'a, T: Clone + Send + Sync + 'static>(&'a GenericColumnReader<T>);

impl PrimaryKeyReader {
    pub fn new(primary_key_reader: Arc<dyn ColumnReader>) -> Self {
        Self(primary_key_reader)
    }

    pub fn typed_reader<T: Clone + Send + Sync + 'static>(
        &self,
    ) -> Option<TypedPrimaryKeyReader<T>> {
        self.0.downcast_ref().map(|r| TypedPrimaryKeyReader(r))
    }
}

impl<'a, T: Clone + Send + Sync + 'static> TypedPrimaryKeyReader<'a, T> {
    pub fn get(&self, docid: DocId) -> Option<T> {
        self.0.get(docid)
    }
}
