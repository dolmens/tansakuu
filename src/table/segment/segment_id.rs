use std::borrow::Borrow;

use uuid::Uuid;

use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Hash, Default, Debug, Serialize, Deserialize)]
pub struct SegmentId(String);

impl SegmentId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().as_simple().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<String> for SegmentId {
    fn from(value: String) -> Self {
        SegmentId(value)
    }
}

impl Borrow<str> for SegmentId {
    fn borrow(&self) -> &str {
        &self.0
    }
}
