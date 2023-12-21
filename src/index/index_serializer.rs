use std::path::Path;

pub trait IndexSerializer {
    fn serialize(&self,  directory: &Path);
}
