use std::path::Path;

use crate::{Directory, DocId};

pub trait ColumnSerializer {
    fn serialize(
        &self,
        directory: &dyn Directory,
        column_directory: &Path,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    );
}
