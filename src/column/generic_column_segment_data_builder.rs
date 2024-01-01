use std::{
    fs::File,
    io::{BufRead, BufReader},
    marker::PhantomData,
    str::FromStr,
};

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
        path: &std::path::Path,
    ) -> Box<dyn super::ColumnSegmentData> {
        let mut values = vec![];
        let file = File::open(path).unwrap();
        let file_reader = BufReader::new(file);
        for line in file_reader.lines() {
            let line = line.unwrap();
            values.push(T::from_str(&line).ok().unwrap());
        }

        Box::new(GenericColumnPersistentSegmentData::new(values))
    }
}
