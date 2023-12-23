use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SegmentMeta {
    doc_count: usize,
}

impl SegmentMeta {
    pub fn new(doc_count: usize) -> Self {
        Self { doc_count }
    }

    pub fn load(path: impl AsRef<Path>) -> Self {
        let json = fs::read_to_string(path.as_ref()).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    pub fn save(&self, path: impl AsRef<Path>) {
        let json = serde_json::to_string_pretty(self).unwrap();
        fs::write(path, json).unwrap();
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count
    }
}
