use crate::{index::IndexSegmentDataBuilder, schema::Index, Directory};

use super::{UniqueKeyDict, UniqueKeyPersistentSegmentData};

pub struct UniqueKeySegmentDataBuilder {}

impl UniqueKeySegmentDataBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl IndexSegmentDataBuilder for UniqueKeySegmentDataBuilder {
    fn build(
        &self,
        index: &Index,
        directory: &dyn Directory,
        index_directory: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let index_path = index_directory.join(index.name());
        let index_data = directory.open_read(&index_path).unwrap();
        let keys = UniqueKeyDict::open(index_data).unwrap();

        Box::new(UniqueKeyPersistentSegmentData::new(keys))
    }
}
