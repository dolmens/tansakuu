#[macro_use]
extern crate log;

#[macro_use]
extern crate thiserror;

pub mod error;

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

pub type VersionId = u64;

pub type DocId = u32;
pub const END_DOCID: DocId = DocId::MAX - 1;
pub const INVALID_DOCID: DocId = DocId::MAX;

pub const END_POSITION: u32 = u32::MAX - 1;
pub const INVALID_POSITION: u32 = u32::MAX;

pub const POSTING_BLOCK_LEN: usize = 128;
pub const SKIPLIST_BLOCK_LEN: usize = 32;
pub const POSITION_BLOCK_LEN: usize = 128;

pub const INVALID_VERSION_ID: VersionId = 0;

mod future_result;

pub use crate::error::TansakuuError;
pub use crate::future_result::FutureResult;

pub type Result<T> = std::result::Result<T, TansakuuError>;

use std::fmt;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

pub use crate::directory::Directory;

const INDEX_FORMAT_VERSION: u32 = 0;
// const INDEX_FORMAT_OLDEST_SUPPORTED_VERSION: u32 = 0;

/// Structure version for the index.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    major: u32,
    minor: u32,
    patch: u32,
    index_format_version: u32,
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

static VERSION: Lazy<Version> = Lazy::new(|| Version {
    major: env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap(),
    minor: env!("CARGO_PKG_VERSION_MINOR").parse().unwrap(),
    patch: env!("CARGO_PKG_VERSION_PATCH").parse().unwrap(),
    index_format_version: INDEX_FORMAT_VERSION,
});

impl ToString for Version {
    fn to_string(&self) -> String {
        format!(
            "tantivy v{}.{}.{}, index_format v{}",
            self.major, self.minor, self.patch, self.index_format_version
        )
    }
}

static VERSION_STRING: Lazy<String> = Lazy::new(|| VERSION.to_string());

/// Expose the current version of tantivy as found in Cargo.toml during compilation.
/// eg. "0.11.0" as well as the compression scheme used in the docstore.
pub fn version() -> &'static Version {
    &VERSION
}

/// Exposes the complete version of tantivy as found in Cargo.toml during compilation as a string.
/// eg. "tantivy v0.11.0, index_format v1, store_compression: lz4".
pub fn version_string() -> &'static str {
    VERSION_STRING.as_str()
}
