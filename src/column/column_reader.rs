use downcast_rs::{impl_downcast, DowncastSync};

use crate::{table::TableDataSnapshot, RowId};

pub trait ColumnReader: DowncastSync {}
impl_downcast!(sync ColumnReader);

pub trait TypedColumnReader: ColumnReader {
    type Item;
    fn get(&self, rowid: RowId, data_snapshot: &TableDataSnapshot) -> Option<Self::Item>;
}

pub struct ColumnReaderSnapshot<'a> {
    data_snapshot: &'a TableDataSnapshot,
    column_reader: &'a dyn ColumnReader,
}

pub struct TypedColumnReaderSnapshot<'a, T, R: TypedColumnReader<Item = T>> {
    data_snapshot: &'a TableDataSnapshot,
    column_reader: &'a R,
}

impl<'a> ColumnReaderSnapshot<'a> {
    pub fn new(data_snapshot: &'a TableDataSnapshot, column_reader: &'a dyn ColumnReader) -> Self {
        Self {
            data_snapshot,
            column_reader,
        }
    }

    pub fn downcast<T, R: TypedColumnReader<Item = T>>(
        &self,
    ) -> Option<TypedColumnReaderSnapshot<'_, T, R>> {
        Some(TypedColumnReaderSnapshot {
            data_snapshot: &self.data_snapshot,
            column_reader: self.column_reader.downcast_ref()?,
        })
    }
}
impl<'a, T, R: TypedColumnReader<Item = T>> TypedColumnReaderSnapshot<'a, T, R> {
    pub fn new(data_snapshot: &'a TableDataSnapshot, column_reader: &'a R) -> Self {
        Self {
            data_snapshot,
            column_reader,
        }
    }

    pub fn get(&self, rowid: RowId) -> Option<T> {
        self.column_reader.get(rowid, self.data_snapshot)
    }
}
