use std::path::PathBuf;

use crate::{Directory, VersionId};

use serde::{Deserialize, Serialize};

use super::segment::SegmentId;

#[derive(Serialize, Deserialize, Clone, PartialEq, Default, Debug)]
pub struct Version {
    version_id: VersionId,
    segments: Vec<SegmentId>,
}

impl Version {
    pub fn load_lastest(directory: &dyn Directory) -> crate::Result<Self> {
        let mut versions: Vec<_> = directory
            .list_files()?
            .into_iter()
            .filter(|path| {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name) = file_name.to_str() {
                        return file_name.starts_with("Version.");
                    }
                }
                false
            })
            .collect();

        if versions.is_empty() {
            return Ok(Version::default());
        }

        versions.sort();

        let path = versions.last().unwrap();
        let version_data = directory.atomic_read(path).unwrap();
        Ok(serde_json::from_slice(&version_data).unwrap())
    }

    #[cfg(not(miri))]
    pub fn next_version(&self) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let version_id = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as VersionId)
            .max(self.version_id + 1);

        Self {
            version_id,
            segments: self.segments.clone(),
        }
    }

    #[cfg(miri)]
    pub fn next_version(&self) -> Self {
        let version_id = self.version_id + 1;

        Self {
            version_id,
            segments: self.segments.clone(),
        }
    }

    pub fn save(&self, directory: &dyn Directory) {
        let path = PathBuf::from(format!("Version.{}", self.version_id));
        let json = serde_json::to_string_pretty(self).unwrap();
        directory.atomic_write(&path, json.as_bytes()).unwrap();
    }

    pub fn version_id(&self) -> VersionId {
        self.version_id
    }

    pub fn segments(&self) -> &[SegmentId] {
        &self.segments
    }

    pub fn remove_segment(&mut self, segment: &SegmentId) {
        self.segments.retain(|s| s != segment);
    }

    pub fn add_segment(&mut self, segment: SegmentId) {
        self.segments.push(segment);
    }
}
