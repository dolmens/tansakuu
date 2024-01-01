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

pub const DOC_BLOCK_LEN: usize = 128;
pub const INVALID_VERSION_ID: VersionId = 0;
