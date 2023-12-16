pub mod postings;
pub mod util;

pub type DocId = i32;
pub type TermFreq = i32;
pub type DocFreq = i32;
pub type FieldMask = u8;

pub const DOC_BLOCK_LEN: usize = 128;
