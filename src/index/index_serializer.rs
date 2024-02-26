use std::path::Path;

use crate::{Directory, DocId};

pub trait IndexSerializer {
    fn serialize(
        &self,
        directory: &dyn Directory,
        index_directory: &Path,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    );
}
