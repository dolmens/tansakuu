use std::path::Path;

use crate::{deletionmap::BuildingDeletionMap, Directory};

pub trait ColumnSerializer {
    fn serialize(
        &self,
        directory: &dyn Directory,
        column_directory: &Path,
        deletionmap: &BuildingDeletionMap,
    );
}
