use std::{fs::File, sync::Arc};

use tantivy_common::file_slice::{FileSlice, WrapFile};

use crate::{index::IndexSegmentDataBuilder, schema::Index};

use super::{PrimaryKeyDict, PrimaryKeyPersistentSegmentData};

pub struct PrimaryKeySegmentDataBuilder {}

impl PrimaryKeySegmentDataBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl IndexSegmentDataBuilder for PrimaryKeySegmentDataBuilder {
    fn build(
        &self,
        index: &Index,
        directory: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let index_path = directory.join(index.name());
        let index_file = File::open(index_path).unwrap();
        let index_data = FileSlice::new(Arc::new(WrapFile::new(index_file).unwrap()));
        let keys = PrimaryKeyDict::open(index_data).unwrap();

        Box::new(PrimaryKeyPersistentSegmentData::new(keys))
    }
}
