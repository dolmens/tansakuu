use std::path::Path;

use crate::{deletionmap::BuildingDeletionMap, Directory};

pub trait IndexSerializer {
    fn serialize(
        &self,
        directory: &dyn Directory,
        index_directory: &Path,
        deletionmap: &BuildingDeletionMap,
    );
}
