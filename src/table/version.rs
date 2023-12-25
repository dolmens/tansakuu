use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::VersionId;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, PartialEq, Default, Debug)]
pub struct Version {
    version_id: VersionId,
    segments: Vec<String>,
}

impl Version {
    pub fn load_lastest(directory: impl AsRef<Path>) -> Self {
        let directory = directory.as_ref();
        let entries = fs::read_dir(directory).unwrap();
        let mut versions: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().is_file()
                    && entry
                        .file_name()
                        .to_str()
                        .map_or(false, |name| name.starts_with("version."))
            })
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .unwrap()
                    .trim_start_matches("version.")
                    .parse::<VersionId>()
                    .ok()
            })
            .collect();
        versions.sort();
        versions.last().map_or(Version::default(), |&version_id| {
            let version_file = directory.join(format!("version.{}", version_id));
            let version_data = fs::read_to_string(version_file).unwrap();
            serde_json::from_str(&version_data).unwrap()
        })
    }

    pub fn new_version(&self) -> Self {
        let version_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as VersionId;

        Self {
            version_id,
            segments: self.segments.clone(),
        }
    }

    pub fn save(&self, directory: impl AsRef<Path>) {
        let version_file_path = directory
            .as_ref()
            .join(format!("version.{}", self.version_id));
        let json = serde_json::to_string_pretty(self).unwrap();
        fs::write(version_file_path, json).unwrap();
    }

    pub fn version_id(&self) -> VersionId {
        self.version_id
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn remove_segment(&mut self, segment: &str) {
        self.segments.retain(|s| s != segment);
    }

    pub fn add_segment(&mut self, segment: String) {
        self.segments.push(segment);
    }
}
