use std::{fs::File, io::Write, marker::PhantomData, path::Path};

pub struct GenericColumnSerializerWriter<T> {
    file: File,
    _marker: PhantomData<T>,
}

impl<T: ToString> GenericColumnSerializerWriter<T> {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            file: File::create(path).unwrap(),
            _marker: PhantomData,
        }
    }

    pub fn write(&mut self, value: T) {
        writeln!(self.file, "{}", value.to_string()).unwrap();
    }
}
