use std::{
    io::{BufRead, BufReader},
    marker::PhantomData,
    str::FromStr,
};

use crate::Directory;

use super::{ColumnSegmentDataBuilder, GenericColumnPersistentSegmentData};

pub struct GenericColumnSegmentDataBuilder<T> {
    _phan: PhantomData<T>,
}

impl<T> GenericColumnSegmentDataBuilder<T> {
    pub fn new() -> Self {
        Self { _phan: PhantomData }
    }
}

impl<T: FromStr + Send + Sync + 'static> ColumnSegmentDataBuilder
    for GenericColumnSegmentDataBuilder<T>
{
    fn build(
        &self,
        field: &crate::schema::Field,
        directory: &dyn Directory,
        path: &std::path::Path,
    ) -> Box<dyn super::ColumnSegmentData> {
        let _ = field;
        let mut values = vec![];
        let file = directory.open_read(path).unwrap();
        let data = file.read_bytes().unwrap();
        let file_reader = BufReader::new(data);
        for line in file_reader.lines() {
            let line = line.unwrap();
            values.push(T::from_str(&line).ok().unwrap());
        }

        Box::new(GenericColumnPersistentSegmentData::new(values))
    }
}
