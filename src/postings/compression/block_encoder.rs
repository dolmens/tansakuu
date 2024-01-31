use std::io::{self, Read, Write};

pub struct BlockEncoder;

impl BlockEncoder {
    pub fn encode_u32<W: Write>(&self, input: &[u32], writer: &mut W) -> io::Result<usize> {
        let mut bytes_written = 0;
        for &v in input {
            let mut to_encode: u32 = v;
            loop {
                let mut next_byte: u8 = (to_encode % 128u32) as u8;
                to_encode /= 128u32;
                if to_encode == 0u32 {
                    next_byte |= 128u8;
                }
                writer.write(&[next_byte])?;
                bytes_written += 1;
                if to_encode == 0u32 {
                    break;
                }
            }
        }

        Ok(bytes_written)
    }

    pub fn encode_u8<W: Write>(&self, input: &[u8], writer: &mut W) -> io::Result<usize> {
        writer.write_all(input)?;
        Ok(input.len())
    }

    pub fn decode_u32<R: Read>(&self, reader: &mut R, output_arr: &mut [u32]) -> io::Result<usize> {
        let mut num_read_bytes = 0;
        for output_mut in output_arr.iter_mut() {
            let mut result = 0u32;
            let mut shift = 0u32;
            loop {
                let mut buf = [0_u8; 1];
                let sz = reader.read(&mut buf)?;
                assert_eq!(sz, 1);
                let cur_byte = buf[0];
                num_read_bytes += 1;
                result += u32::from(cur_byte % 128u8) << shift;
                if cur_byte & 128u8 != 0u8 {
                    break;
                }
                shift += 7;
            }
            *output_mut = result;
        }

        Ok(num_read_bytes)
    }

    pub fn decode_u8<R: Read>(&self, reader: &mut R, output_arr: &mut [u8]) -> io::Result<usize> {
        reader.read_exact(output_arr)?;
        Ok(output_arr.len())
    }
}
