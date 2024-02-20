use std::path::Path;

use crate::Directory;

pub trait IndexSerializer {
    fn serialize(&self, directory: &dyn Directory, index_directory: &Path);
}
