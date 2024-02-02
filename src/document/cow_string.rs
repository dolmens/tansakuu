use std::{
    borrow::Cow,
    io::{self, Read, Write},
};

use tantivy_common::{BinarySerializable, VInt};

#[derive(Debug)]
pub struct CowString<'a>(Cow<'a, str>);

impl<'a> CowString<'a> {
    pub fn borrowed(s: &'a str) -> Self {
        Self(Cow::Borrowed(s))
    }

    pub fn owned(s: String) -> Self {
        Self(Cow::Owned(s))
    }
}

impl<'a> BinarySerializable for CowString<'a> {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        let data: &[u8] = self.0.as_bytes();
        VInt(data.len() as u64).serialize(writer)?;
        writer.write_all(data)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<CowString<'a>> {
        let string_length = VInt::deserialize(reader)?.val() as usize;
        let mut result = String::with_capacity(string_length);
        reader
            .take(string_length as u64)
            .read_to_string(&mut result)?;
        Ok(CowString(Cow::Owned(result)))
    }
}
