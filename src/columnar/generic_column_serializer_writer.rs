use std::{
    io::{self, Write},
    marker::PhantomData,
};

use crate::directory::WritePtr;

pub struct GenericColumnSerializerWriter<T> {
    writer: WritePtr,
    _marker: PhantomData<T>,
}

impl<T: ToString> GenericColumnSerializerWriter<T> {
    pub fn new(writer: WritePtr) -> Self {
        Self {
            writer,
            _marker: PhantomData,
        }
    }

    pub fn write(&mut self, value: T) {
        writeln!(&mut self.writer, "{}", value.to_string()).unwrap();
    }

    pub fn finish(self) -> io::Result<WritePtr> {
        Ok(self.writer)
    }
}
