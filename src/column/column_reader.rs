use downcast_rs::{impl_downcast, DowncastSync};

use crate::{table::TableDataSnapshot, RowId};

pub trait ColumnReader: DowncastSync {}
impl_downcast!(sync ColumnReader);

pub trait TypedColumnReader: ColumnReader {
    type Item;
    fn get(&self, rowid: RowId, data_snapshot: &TableDataSnapshot) -> Option<Self::Item>;
}

pub struct ColumnReaderSnapshot<'a> {
    reader: &'a dyn ColumnReader,
    snapshot: &'a TableDataSnapshot,
}

pub struct TypedColumnReaderSnapshot<'a, T, R: TypedColumnReader<Item = T>> {
    reader: &'a R,
    snapshot: &'a TableDataSnapshot,
}

impl<'a> ColumnReaderSnapshot<'a> {
    pub fn new(reader: &'a dyn ColumnReader, snapshot: &'a TableDataSnapshot) -> Self {
        Self { reader, snapshot }
    }

    pub fn downcast<T, R: TypedColumnReader<Item = T>>(
        &self,
    ) -> Option<TypedColumnReaderSnapshot<'_, T, R>> {
        Some(TypedColumnReaderSnapshot {
            reader: self.reader.downcast_ref()?,
            snapshot: &self.snapshot,
        })
    }
}
impl<'a, T, R: TypedColumnReader<Item = T>> TypedColumnReaderSnapshot<'a, T, R> {
    pub fn new(reader: &'a R, snapshot: &'a TableDataSnapshot) -> Self {
        Self { reader, snapshot }
    }

    pub fn get(&self, rowid: RowId) -> Option<T> {
        self.reader.get(rowid, self.snapshot)
    }
}
