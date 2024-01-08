pub mod column;
pub mod deletionmap;
pub mod directory;
pub mod document;
pub mod index;
pub mod postings;
pub mod query;
pub mod schema;
pub mod table;
pub mod util;

pub type DocId = u32;
pub type DocUniqueId = u64;
pub type TermFreq = u32;
pub type DocFreq = u32;
pub type FieldMask = u8;
pub type VersionId = u64;

pub const END_DOCID: DocId = DocId::MAX - 1000;
pub const INVALID_DOCID: DocId = END_DOCID + 1;

pub const DOCLIST_BLOCK_LEN: usize = 128;
pub const SKIPLIST_BLOCK_LEN: usize = 32;

pub const INVALID_VERSION_ID: VersionId = 0;
