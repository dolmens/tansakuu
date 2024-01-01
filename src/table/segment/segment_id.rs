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
}

impl From<String> for SegmentId {
    fn from(value: String) -> Self {
        SegmentId(value)
    }
}
