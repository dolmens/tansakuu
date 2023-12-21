use std::path::Path;

pub trait ColumnSerializer {
    fn serialize(&self, directory: &Path);
}
