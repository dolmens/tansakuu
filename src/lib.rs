pub mod column;
pub mod document;
pub mod index;
pub mod postings;
pub mod query;
pub mod schema;
pub mod table;
pub mod util;
pub mod directory;

pub type DocId = i32;
pub type RowId = usize;
pub type TermFreq = i32;
pub type DocFreq = i32;
pub type FieldMask = u8;

pub const DOC_BLOCK_LEN: usize = 128;
pub const END_DOCID: DocId = 100000000;
