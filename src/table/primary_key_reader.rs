use crate::{
    column::{ColumnReader, ColumnReaderSnapshot, GenericColumnReader, TypedColumnReaderSnapshot},
    DocId,
};

use super::TableDataSnapshot;

pub struct PrimaryKeyReaderSnapshot<'a>(ColumnReaderSnapshot<'a>);

pub struct TypedPrimaryKeyReaderSnapshot<'a, T: Clone + Send + Sync + 'static>(
    TypedColumnReaderSnapshot<'a, T, GenericColumnReader<T>>,
);

impl<'a> PrimaryKeyReaderSnapshot<'a> {
    pub fn new(reader: &'a dyn ColumnReader, snapshot: &'a TableDataSnapshot) -> Self {
        Self(ColumnReaderSnapshot::new(reader, snapshot))
    }

    pub fn get_typed_reader<T: Clone + Send + Sync + 'static>(
        &self,
    ) -> Option<TypedPrimaryKeyReaderSnapshot<'_, T>> {
        self.0
            .downcast::<T, GenericColumnReader<_>>()
            .map(|reader| TypedPrimaryKeyReaderSnapshot(reader))
    }
}

impl<'a, T: Clone + Send + Sync + 'static> TypedPrimaryKeyReaderSnapshot<'a, T> {
    pub fn get(&self, docid: DocId) -> Option<T> {
        self.0.get(docid)
    }
}
