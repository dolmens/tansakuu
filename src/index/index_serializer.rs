use std::{path::Path, sync::Arc};

use crate::{schema::IndexRef, Directory, DocId};

use super::IndexSegmentData;

pub trait IndexSerializer {
    fn serialize(
        &self,
        index: &IndexRef,
        index_data: &Arc<dyn IndexSegmentData>,
        directory: &dyn Directory,
        index_path: &Path,
        docid_mapping: Option<&Vec<Option<DocId>>>,
    );
}
