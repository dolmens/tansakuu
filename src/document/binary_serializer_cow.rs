use std::{
    borrow::Cow,
    io::{self, Read, Write},
};

use tantivy_common::{BinarySerializable, VInt};

// #[derive(Clone, Debug)]
// pub struct BinarySerializableCow<'a, B: ToOwned + ?Sized + 'a>(Cow<'a, B>);

// impl<'a, B: ?Sized + 'a> BinarySerializableCow<'a, B> {
//     pub fn borrowed(b: &'a B) -> Self {
//         Self(Cow::Borrowed(b))
//     }

//     pub fn owned(o: <B as ToOwned>::Owned) -> Self {
//         Self(Cow::Owned(o))
//     }
// }

// impl<'a> BinarySerializable for BinarySerializableCow<'a, str> {
//     fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
//         let data: &[u8] = self.0.as_bytes();
//         VInt(data.len() as u64).serialize(writer)?;
//         writer.write_all(data)
//     }

//     fn deserialize<R: Read>(reader: &mut R) -> io::Result<BinarySerializableCow<'a>> {
//         let string_length = VInt::deserialize(reader)?.val() as usize;
//         let mut result = String::with_capacity(string_length);
//         reader
//             .take(string_length as u64)
//             .read_to_string(&mut result)?;
//         Ok(BinarySerializableCow(Cow::Owned(result)))
//     }
// }
