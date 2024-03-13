use crate::{index::IndexSegmentDataBuilder, schema::Index, Directory};

use super::{UniqueKeyDict, UniqueKeyPersistentSegmentData};

#[derive(Default)]
pub struct UniqueKeySegmentDataBuilder {}

impl IndexSegmentDataBuilder for UniqueKeySegmentDataBuilder {
    fn build(
        &self,
        index: &Index,
        directory: &dyn Directory,
        index_path: &std::path::Path,
    ) -> Box<dyn crate::index::IndexSegmentData> {
        let index_path = index_path.join(index.name());
        let index_data = directory.open_read(&index_path).unwrap();
        let keys = UniqueKeyDict::open(index_data).unwrap();

        Box::new(UniqueKeyPersistentSegmentData::new(keys))
    }
}
