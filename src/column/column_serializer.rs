use std::path::Path;

use crate::Directory;

pub trait ColumnSerializer {
    fn serialize(&self, directory: &dyn Directory, column_directory: &Path);
}
